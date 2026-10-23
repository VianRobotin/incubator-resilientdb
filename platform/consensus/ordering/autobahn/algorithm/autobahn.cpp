#include "platform/consensus/ordering/autobahn/algorithm/autobahn.h"

#include <glog/logging.h>

#include <algorithm>

#include "common/crypto/signature_verifier.h"
#include "common/utils/utils.h"

namespace resdb {
namespace autobahn {

namespace {

constexpr size_t kEchoFlushSize = 256;
constexpr int64_t kEchoFlushUs = 1000;
constexpr size_t kRecentCars = 256;  // committed Cars kept to answer queries
// Blame timers start at the first proposal, or after this many Delta if none
// arrives (replicas of a deployment start within this window).
constexpr int64_t kStartupGraceDeltas = 40;

int64_t Now() { return static_cast<int64_t>(GetCurrentTime()); }

void PutI64(std::string* s, int64_t v) {
  for (int i = 0; i < 8; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}
void PutI32(std::string* s, int32_t v) {
  for (int i = 0; i < 4; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

std::string ZeroHash() { return std::string(32, '\0'); }

}  // namespace

AutoBahn::AutoBahn(int id, int f, int total_num, int block_size,
                   SignatureVerifier* verifier, const Params& params)
    : ProtocolBase(id, f, total_num),
      n_(total_num),
      block_size_(block_size),
      params_(params),
      bof_(params.batch_order_fairness),
      verifier_(verifier),
      lanes_(total_num + 1) {
  global_stats_ = Stats::GetGlobalStats(5);
  tee_ = std::make_unique<TeeHost>();
  CHECK(tee_->Initialize(params_.enclave_path, id_, n_, f_, bof_,
                         params_.group_seed))
      << "Pearl requires its enclave (" << params_.enclave_path << ")";
  if (bof_) {
    bof_orderer_ = std::make_unique<BofOrderer>(n_, f_, params_.bof_gamma);
    bof_theta_ = bof_orderer_->Theta();
  } else {
    ol_ = std::make_unique<OlOrderer>(n_, f_);
  }
  last_car_hash_ = ZeroHash();
  int64_t now = Now();
  last_seal_ = last_activity_ = view_start_ = start_time_ = now;

  LOG(ERROR) << "Pearl-" << (bof_ ? "BOF" : "OL") << " replica " << id_
             << " n=" << n_ << " f=" << f_ << " block_size=" << block_size_
             << " Delta=" << params_.delta_us << "us"
             << (bof_ ? " theta=" + std::to_string(bof_orderer_->Theta()) : "");
}

AutoBahn::~AutoBahn() {
  Stop();
  event_cv_.notify_all();
  exec_cv_.notify_all();
  if (protocol_thread_.joinable()) protocol_thread_.join();
  if (execution_thread_.joinable()) execution_thread_.join();
}

void AutoBahn::Start() {
  protocol_thread_ = std::thread(&AutoBahn::ProtocolLoop, this);
  execution_thread_ = std::thread(&AutoBahn::ExecutionLoop, this);
}

// ============================================================
// Protocol thread
// ============================================================

void AutoBahn::Post(std::function<void()> fn) {
  {
    std::lock_guard<std::mutex> lk(event_mutex_);
    events_.push_back(std::move(fn));
  }
  event_cv_.notify_one();
}

// Messages signed by a replica whose public key is not known yet fail
// verification, so the protocol starts only once every replica's key arrived
// (until then incoming events stay queued).
bool AutoBahn::NetworkReady() {
  if (network_ready_) return true;
  for (int j = 1; j <= n_; ++j) {
    if (!verifier_->GetPublicKey(j).ok()) return false;
  }
  network_ready_ = true;
  int64_t now = Now();
  start_time_ = last_seal_ = last_activity_ = view_start_ = now;
  LOG(ERROR) << "Pearl replica " << id_ << ": all public keys known, starting";
  return true;
}

void AutoBahn::ProtocolLoop() {
  while (!IsStop() && !NetworkReady()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  while (!IsStop()) {
    std::deque<std::function<void()>> batch;
    {
      std::unique_lock<std::mutex> lk(event_mutex_);
      event_cv_.wait_for(lk, std::chrono::milliseconds(1),
                         [&] { return !events_.empty() || IsStop(); });
      batch.swap(events_);
    }
    for (auto& fn : batch) fn();
    Tick();
  }
}

void AutoBahn::Tick() {
  int64_t now = Now();
  CloseCollectionWindows();
  MaybeHeartbeat();
  MaybeSealCar();
  if (!echo_out_.empty() && now - last_echo_flush_ >= kEchoFlushUs) {
    FlushEchoes();
  }
  RequestMissingCars();
  CheckDelinquencyTimers();
  ProcessPendingVotes();
  CheckCommitTimers();
  ProcessPendingCommits();
  CheckBlame();
  if (!in_view_ && now - quit_time_ >= 2 * params_.delta_us) EnterNextView();
  MaybePropose();
}

// ============================================================
// Dissemination layer (Sec. 5.2, Alg. 1)
// ============================================================

bool AutoBahn::ReceiveTransaction(std::unique_ptr<Transaction> txn) {
  int64_t now = Now();
  txn->set_create_time(now);
  {
    std::lock_guard<std::mutex> lk(create_mutex_);
    create_times_.emplace(txn->hash(), now);
  }
  auto t = std::make_shared<Transaction>(std::move(*txn));
  Post([this, t] { OnFirstReceive(*t); });
  return true;
}

// OnFirstReceive (Alg. 1): attest t in the TEE and echo it with the evidence.
void AutoBahn::OnFirstReceive(const Transaction& txn) {
  const std::string& id = txn.hash();
  int64_t now = Now();
  last_activity_ = now;
  if (!seen_.insert(id).second) return;

  TeeHost::SignedIndicator own;
  own.signer = id_;
  bool attested = bof_ ? tee_->BofPlace(id, &own.value, &own.tag)
                       : tee_->OlAttest(id, &own.value, &own.tag);
  if (!attested) return;  // already in the enclave's history H

  TxnRecord& rec = txns_[id];
  rec.first_seen = now;
  rec.evidence[id_] = own;
  bodies_[id] = txn;
  windows_.emplace_back(now + 2 * params_.delta_us, id);
  QueueEcho(txn, own.value, own.tag);

  if (bof_ && ++open_count_ >= block_size_) SealCar();
}

void AutoBahn::QueueEcho(const Transaction& txn, int64_t value,
                         const std::string& tag) {
  Echo echo;
  *echo.mutable_txn() = txn;
  echo.set_value(value);
  echo.set_tee_tag(tag);
  echo_out_.push_back(std::move(echo));
  if (echo_out_.size() >= kEchoFlushSize) FlushEchoes();
}

// Echoes are sent in small batches; each echo is still an individual
// retransmission of t with this replica's evidence.
void AutoBahn::FlushEchoes() {
  if (echo_out_.empty()) return;
  EchoBatch batch;
  batch.set_sender_id(id_);
  for (auto& e : echo_out_) *batch.add_echoes() = std::move(e);
  echo_out_.clear();
  last_echo_flush_ = Now();
  Broadcast(MessageType::Echo_Batch, batch);
}

// OnReceiveOLEcho / OnReceiveBOFEcho: verify, then store.  Verification is
// stateless and runs on the network thread.
void AutoBahn::ReceiveEchoBatch(std::unique_ptr<EchoBatch> batch) {
  const int sender = batch->sender_id();
  if (sender == id_ || sender < 1 || sender > n_) return;
  std::vector<TeeHost::VerifyItem> items;
  std::vector<int> index;
  for (int i = 0; i < batch->echoes_size(); ++i) {
    const Echo& e = batch->echoes(i);
    if (SignatureVerifier::CalculateHash(e.txn().data()) != e.txn().hash()) {
      continue;
    }
    items.push_back({bof_ ? TeeHost::BofPlacementMsg(sender, e.txn().hash(), e.value())
                          : TeeHost::OlTimestampMsg(sender, e.txn().hash(), e.value()),
                     e.tee_tag()});
    index.push_back(i);
  }
  std::vector<bool> ok = tee_->VerifyBatch(items);
  auto verified = std::make_shared<EchoBatch>();
  verified->set_sender_id(sender);
  for (size_t k = 0; k < index.size(); ++k) {
    if (ok[k]) *verified->add_echoes() = std::move(*batch->mutable_echoes(index[k]));
  }
  if (verified->echoes_size() == 0) return;
  Post([this, verified] { HandleEchoBatch(*verified); });
}

void AutoBahn::HandleEchoBatch(const EchoBatch& batch) {
  for (const Echo& e : batch.echoes()) {
    OnFirstReceive(e.txn());
    auto it = txns_.find(e.txn().hash());
    if (it == txns_.end()) continue;  // window already closed
    TeeHost::SignedIndicator ind;
    ind.signer = batch.sender_id();
    ind.value = e.value();
    ind.tag = e.tee_tag();
    it->second.evidence.emplace(ind.signer, std::move(ind));
  }
}

// AfterCollectionWindow (Alg. 1): 2 Delta after first reception.
//   OL:  the enclave reduces the collected timestamps to K_r(t), which is
//        placed in the next Car.
//   BOF: the enclave records the collected placement announcements, closing
//        t's window and advancing (w_r, M_r).
void AutoBahn::CloseCollectionWindows() {
  int64_t now = Now();
  auto process = [&](std::deque<std::pair<int64_t, std::string>>& queue) {
    while (!queue.empty() && queue.front().first <= now) {
      std::string id = queue.front().second;
      queue.pop_front();
      auto it = txns_.find(id);
      if (it == txns_.end()) continue;
      std::vector<TeeHost::SignedIndicator> ev;
      ev.reserve(it->second.evidence.size());
      for (const auto& e : it->second.evidence) ev.push_back(e.second);

      if (bof_) {
        if (!tee_->BofRecordAnnouncements(id, ev)) {
          LOG(ERROR) << "BOF: enclave refused announcements for a transaction";
        }
        txns_.erase(it);
        continue;
      }
      int64_t key = 0;
      if (!tee_->OlLocalKey(id, ev, &key)) {
        // Fewer than f+1 genuine timestamps so far: not reachable when the
        // synchrony bound holds.  Keep collecting and retry; the completion
        // frontier cannot pass t meanwhile.
        retry_windows_.emplace_back(now + params_.delta_us, id);
        continue;
      }
      txns_.erase(it);
      ol_open_.push_back({id, key});
      if (static_cast<int>(ol_open_.size()) >= block_size_) SealCar();
    }
  };
  process(windows_);
  process(retry_windows_);
}

// Heartbeat (Sec. 5.2): when no transaction has arrived for a configurable
// period, have the enclave issue a fresh indicator so frontiers keep moving.
void AutoBahn::MaybeHeartbeat() {
  int64_t now = Now();
  if (now - last_activity_ < params_.heartbeat_us ||
      now - last_heartbeat_ < params_.heartbeat_us) {
    return;
  }
  std::string id;
  int64_t value = 0;
  if (!tee_->Heartbeat(&id, &value)) return;
  last_heartbeat_ = now;
  if (bof_) {
    ++open_count_;
  } else {
    heartbeat_since_seal_ = true;
  }
}

// A Car is sealed once full (checked on insertion) or once X = Delta has
// elapsed since the previous seal.
void AutoBahn::MaybeSealCar() {
  bool nonempty = bof_ ? open_count_ > 0
                       : (!ol_open_.empty() || heartbeat_since_seal_);
  if (nonempty && Now() - last_seal_ >= params_.delta_us) SealCar();
}

// OnCarReady (Alg. 1): the enclave seals the Car and its frontier; the Car
// also carries the PoA of this lane's latest certified Car.
void AutoBahn::SealCar() {
  TeeHost::SealedCar sealed;
  Car car;
  if (!bof_) {
    std::vector<TeeHost::OlItem> items;
    items.swap(ol_open_);
    if (!tee_->OlSealCar(items, &sealed)) {
      LOG(ERROR) << "OL: enclave refused to seal a Car";
      return;
    }
    for (const auto& it : items) {
      OlCarItem* item = car.add_ol_items();
      item->set_txn_hash(it.id);
      item->set_key(it.key);
      auto body = bodies_.find(it.id);
      CHECK(body != bodies_.end());
      *car.add_txns() = std::move(body->second);
      bodies_.erase(body);
    }
    heartbeat_since_seal_ = false;
  } else {
    if (!tee_->BofSealCar(&sealed)) {
      LOG(ERROR) << "BOF: enclave refused to seal a Car";
      return;
    }
    for (const auto& it : sealed.bof_items) {
      car.add_bof_items(it.id);
      car.add_bof_heartbeat(it.heartbeat);
      if (it.heartbeat) continue;
      auto body = bodies_.find(it.id);
      CHECK(body != bodies_.end());
      *car.add_txns() = std::move(body->second);
      bodies_.erase(body);
    }
    for (int j = 1; j <= n_; ++j) {
      Announcement* a = car.add_m();
      const TeeHost::Announcement& m = sealed.m[j];
      a->set_present(m.present);
      if (!m.present) continue;
      a->set_car(m.car);
      a->set_txn_hash(m.id);
      a->set_tee_tag(m.tag);
    }
    open_count_ = 0;
  }

  car.set_lane(id_);
  car.set_index(sealed.index);
  car.set_prev_hash(last_car_hash_);
  car.set_hash(sealed.hash);
  car.set_tee_tag(sealed.tag);
  car.set_frontier(sealed.frontier);
  car.set_seal_time(Now());
  if (own_poa_.index() > 0) {
    *car.mutable_poa() = own_poa_;
    Lane& own = lanes_[id_];
    if (own.certified_at.empty() || own.certified_at.back().first < own_poa_.index()) {
      own.certified_at.emplace_back(own_poa_.index(), Now());
    }
  }
  last_car_hash_ = sealed.hash;
  last_seal_ = Now();

  Broadcast(MessageType::Car_Prop, car);

  Lane& lane = lanes_[id_];
  lane.hashes[car.index()] = car.hash();
  lane.acked = car.index();
  CarAck self;
  self.set_lane(id_);
  self.set_index(car.index());
  self.set_car_hash(car.hash());
  *self.mutable_sign() = SignAck(car);
  lane.cars.emplace(car.index(), std::move(car));
  HandleCarAck(self);
}

SignInfo AutoBahn::SignAck(const Car& car) {
  SignInfo info;
  info.set_hash(car.hash());
  info.set_sender_id(id_);
  info.set_local_id(car.index());
  auto sig = verifier_->SignMessage(car.hash());
  if (sig.ok()) *info.mutable_sign() = *sig;
  return info;
}

bool AutoBahn::VerifyPoa(int lane, const Poa& poa) {
  std::set<int> signers;
  for (const SignInfo& ack : poa.acks()) {
    if (ack.hash() != poa.car_hash() || ack.local_id() != poa.index()) continue;
    if (ack.sender_id() < 1 || ack.sender_id() > n_) continue;
    if (ack.sign().node_id() != ack.sender_id()) continue;
    if (!verifier_->VerifyMessage(poa.car_hash(), ack.sign())) continue;
    signers.insert(ack.sender_id());
  }
  return static_cast<int>(signers.size()) >= f_ + 1;
}

// Stateless checks of a received Car: its hash matches its content, the
// lane's enclave sealed it, bodies match their identifiers, and the embedded
// PoA holds f+1 acknowledgments.
bool AutoBahn::VerifyCar(const Car& car) {
  if (car.lane() < 1 || car.lane() > n_ || car.index() < 1) return false;
  std::string hash;
  if (!bof_) {
    if (car.txns_size() != car.ol_items_size()) return false;
    std::vector<TeeHost::OlItem> items;
    for (int i = 0; i < car.ol_items_size(); ++i) {
      const auto& it = car.ol_items(i);
      if (car.txns(i).hash() != it.txn_hash()) return false;
      items.push_back({it.txn_hash(), it.key()});
    }
    hash = TeeHost::OlCarHash(car.lane(), car.index(), car.prev_hash(), items,
                              car.frontier());
  } else {
    if (car.bof_items_size() != car.bof_heartbeat_size() ||
        car.m_size() != n_) {
      return false;
    }
    std::vector<TeeHost::BofItem> items;
    int body = 0;
    for (int i = 0; i < car.bof_items_size(); ++i) {
      items.push_back({car.bof_items(i), car.bof_heartbeat(i)});
      if (car.bof_heartbeat(i)) continue;
      if (body >= car.txns_size() || car.txns(body).hash() != car.bof_items(i)) {
        return false;
      }
      ++body;
    }
    if (body != car.txns_size()) return false;
    std::vector<TeeHost::Announcement> m(n_ + 1);
    for (int j = 1; j <= n_; ++j) {
      const Announcement& a = car.m(j - 1);
      m[j].present = a.present();
      m[j].car = a.car();
      m[j].id = a.txn_hash();
      m[j].tag = a.tee_tag();
    }
    hash = TeeHost::BofCarHash(car.lane(), car.index(), car.prev_hash(), items,
                               car.frontier(), m, n_);
  }
  if (hash != car.hash()) return false;
  for (const Transaction& txn : car.txns()) {
    if (SignatureVerifier::CalculateHash(txn.data()) != txn.hash()) return false;
  }
  if (car.poa().index() > 0 &&
      (car.poa().index() >= car.index() || !VerifyPoa(car.lane(), car.poa()))) {
    return false;
  }
  return tee_->VerifyBatch({{TeeHost::CarSigMsg(car.lane(), hash), car.tee_tag()}})[0];
}

void AutoBahn::ReceiveCar(std::unique_ptr<Car> car) {
  if (car->lane() == id_) return;
  if (!VerifyCar(*car)) {
    LOG(ERROR) << "dropping invalid Car " << car->index() << " of lane "
               << car->lane();
    return;
  }
  auto c = std::make_shared<Car>(std::move(*car));
  Post([this, c] { HandleCar(*c); });
}

// OnReceiveProp (Alg. 1): store the Car and acknowledge it, but only once
// the Car at position i-1 of the lane was received and acknowledged.
void AutoBahn::HandleCar(const Car& car) {
  Lane& lane = lanes_[car.lane()];
  if (car.poa().index() > 0) LearnCertified(car.lane(), car.poa());
  const int64_t idx = car.index();
  if (idx <= lane.acked || lane.buffered.count(idx)) return;
  // Transactions received for the first time in a Car (Sec. 5.2).
  for (const Transaction& txn : car.txns()) OnFirstReceive(txn);
  lane.buffered.emplace(idx, car);
  while (!lane.buffered.empty() && lane.buffered.begin()->first == lane.acked + 1) {
    Car next = std::move(lane.buffered.begin()->second);
    lane.buffered.erase(lane.buffered.begin());
    std::string prev = next.index() == 1 ? ZeroHash() : lane.hashes[next.index() - 1];
    if (next.prev_hash() != prev) {
      LOG(ERROR) << "lane " << next.lane() << " Car " << next.index()
                 << " does not extend the acknowledged prefix";
      continue;
    }
    AcceptCar(lane, std::move(next));
  }
}

void AutoBahn::AcceptCar(Lane& lane, Car car) {
  CarAck ack;
  ack.set_lane(car.lane());
  ack.set_index(car.index());
  ack.set_car_hash(car.hash());
  *ack.mutable_sign() = SignAck(car);
  SendMessage(MessageType::Car_Ack, ack, car.lane());
  lane.acked = car.index();
  lane.hashes[car.index()] = car.hash();
  lane.cars.emplace(car.index(), std::move(car));
}

void AutoBahn::LearnCertified(int lane_id, const Poa& poa) {
  Lane& lane = lanes_[lane_id];
  if (poa.index() <= lane.certified.index()) return;
  lane.certified = poa;
  int64_t now = Now();
  lane.certified_at.emplace_back(poa.index(), now);
  while (lane.certified_at.size() > 1 &&
         lane.certified_at[1].second <= now - 4 * params_.delta_us) {
    lane.certified_at.pop_front();
  }
}

void AutoBahn::ReceiveCarAck(std::unique_ptr<CarAck> ack) {
  if (ack->lane() != id_) return;
  const SignInfo& s = ack->sign();
  if (s.hash() != ack->car_hash() || s.local_id() != ack->index() ||
      s.sign().node_id() != s.sender_id() ||
      !verifier_->VerifyMessage(ack->car_hash(), s.sign())) {
    return;
  }
  auto a = std::make_shared<CarAck>(std::move(*ack));
  Post([this, a] { HandleCarAck(*a); });
}

// f+1 acknowledgments of an own Car form its Proof of Availability.
void AutoBahn::HandleCarAck(const CarAck& ack) {
  Lane& lane = lanes_[id_];
  auto h = lane.hashes.find(ack.index());
  if (h == lane.hashes.end() || h->second != ack.car_hash()) return;
  if (ack.index() <= own_poa_.index()) return;
  auto& acks = own_acks_[ack.index()];
  acks.emplace(ack.sign().sender_id(), ack.sign());
  if (static_cast<int>(acks.size()) < f_ + 1) return;
  Poa poa;
  poa.set_index(ack.index());
  poa.set_car_hash(ack.car_hash());
  for (const auto& a : acks) *poa.add_acks() = a.second;
  own_poa_ = poa;
  own_acks_.erase(own_acks_.begin(), own_acks_.upper_bound(ack.index()));
  // Usable at once (proposals, timers), but other replicas only learn it
  // from the next Car; freshness counts it from that dissemination.
  lanes_[id_].certified = poa;
}

void AutoBahn::RequestCar(int lane_id, int64_t index) {
  Lane& lane = lanes_[lane_id];
  int64_t now = Now();
  auto& last = lane.queried[index];
  if (now - last < 2 * params_.delta_us) return;
  last = now;
  CarQuery q;
  q.set_lane(lane_id);
  q.set_index(index);
  q.set_requester(id_);
  Broadcast(MessageType::Car_Query, q);
}

// Fill gaps below buffered Cars (lost messages) from any replica holding them.
void AutoBahn::RequestMissingCars() {
  int64_t now = Now();
  for (int j = 1; j <= n_; ++j) {
    Lane& lane = lanes_[j];
    if (j == id_) continue;
    bool gap = !lane.buffered.empty() &&
               lane.buffered.begin()->first > lane.acked + 1;
    if (!gap) {
      lane.buffer_since = 0;
      continue;
    }
    if (lane.buffer_since == 0) lane.buffer_since = now;
    if (now - lane.buffer_since < 2 * params_.delta_us) continue;
    for (int64_t idx = lane.acked + 1;
         idx < lane.buffered.begin()->first && idx <= lane.acked + 32; ++idx) {
      RequestCar(j, idx);
    }
  }
}

void AutoBahn::ReceiveCarQuery(std::unique_ptr<CarQuery> query) {
  auto q = std::make_shared<CarQuery>(std::move(*query));
  Post([this, q] {
    if (q->lane() < 1 || q->lane() > n_ || q->requester() == id_) return;
    const Lane& lane = lanes_[q->lane()];
    const Car* car = nullptr;
    auto it = lane.cars.find(q->index());
    if (it != lane.cars.end()) car = &it->second;
    auto rt = lane.recent.find(q->index());
    if (car == nullptr && rt != lane.recent.end()) car = &rt->second;
    if (car != nullptr) SendMessage(MessageType::Car_Prop, *car, q->requester());
  });
}

const Car* AutoBahn::FindCar(int lane_id, int64_t index) const {
  const Lane& lane = lanes_[lane_id];
  auto it = lane.cars.find(index);
  if (it != lane.cars.end()) return &it->second;
  auto rt = lane.recent.find(index);
  if (rt != lane.recent.end()) return &rt->second;
  return nullptr;
}

// ============================================================
// Consensus layer (Sec. 5.3, Alg. 2): Sync HotStuff over tip cuts
// ============================================================

int AutoBahn::LeaderOf(int64_t view) const {
  return static_cast<int>((view - 1) % n_) + 1;
}

bool AutoBahn::RankAbove(const QuorumCert& a, const QuorumCert& b) {
  if (a.view() != b.view()) return a.view() > b.view();
  return a.height() > b.height();
}

std::string AutoBahn::BlockDigest(const ConsensusBlock& b) const {
  std::string s = "pearl-block";
  PutI64(&s, b.view());
  PutI64(&s, b.height());
  s += b.parent_hash();
  PutI32(&s, b.proposer());
  for (const Tip& t : b.tips()) {
    PutI32(&s, t.lane());
    PutI64(&s, t.index());
    s += t.car_hash();
  }
  s += "claims";
  for (const Claim& c : b.claims()) {
    PutI32(&s, c.lane());
    PutI64(&s, c.position());
  }
  return TeeHost::Sha256(s);
}

std::string AutoBahn::VoteDigest(int64_t view, int64_t height,
                                 const std::string& hash) const {
  std::string s = "pearl-vote";
  PutI64(&s, view);
  PutI64(&s, height);
  s += hash;
  return TeeHost::Sha256(s);
}

std::string AutoBahn::BlameDigest(int64_t view) const {
  std::string s = "pearl-blame";
  PutI64(&s, view);
  return TeeHost::Sha256(s);
}

bool AutoBahn::VerifyVote(const Vote& v) {
  if (v.sender_id() < 1 || v.sender_id() > n_ ||
      v.sign().node_id() != v.sender_id()) {
    return false;
  }
  return verifier_->VerifyMessage(VoteDigest(v.view(), v.height(), v.block_hash()),
                                  v.sign());
}

bool AutoBahn::VerifyQc(const QuorumCert& qc) {
  if (qc.height() == 0) return qc.block_hash().empty();  // genesis
  std::set<int> signers;
  for (const Vote& v : qc.votes()) {
    if (v.view() != qc.view() || v.height() != qc.height() ||
        v.block_hash() != qc.block_hash() || !VerifyVote(v)) {
      continue;
    }
    signers.insert(v.sender_id());
  }
  return static_cast<int>(signers.size()) >= f_ + 1;
}

// Stateless checks of a proposed block: digest, TEE.SignBlock tag, parent
// certificate, and the PoA of every tip.
bool AutoBahn::VerifyBlock(const ConsensusBlock& b) {
  if (b.proposer() < 1 || b.proposer() > n_ || b.height() < 1) return false;
  if (BlockDigest(b) != b.hash()) return false;
  const QuorumCert& qc = b.parent_qc();
  if (qc.height() != b.height() - 1 || qc.block_hash() != b.parent_hash() ||
      !VerifyQc(qc)) {
    return false;
  }
  std::set<int> lanes;
  for (const Tip& t : b.tips()) {
    if (t.lane() < 1 || t.lane() > n_ || !lanes.insert(t.lane()).second) {
      return false;
    }
    if (t.poa().index() != t.index() || t.poa().car_hash() != t.car_hash() ||
        !VerifyPoa(t.lane(), t.poa())) {
      return false;
    }
  }
  for (const Claim& c : b.claims()) {
    if (!bof_ || c.lane() < 1 || c.lane() > n_) return false;
  }
  return tee_->VerifyBatch({{TeeHost::BlockMsg(b.proposer(), b.view(), b.height(),
                                               b.hash()),
                             b.tee_tag()}})[0];
}

const Tip* AutoBahn::TipOf(const ConsensusBlock& b, int lane) const {
  for (const Tip& t : b.tips()) {
    if (t.lane() == lane) return &t;
  }
  return nullptr;
}

int64_t AutoBahn::TipIndex(const std::string& block_hash, int lane) const {
  if (block_hash.empty()) return 0;  // genesis
  auto it = blocks_.find(block_hash);
  if (it == blocks_.end()) return 0;
  const Tip* t = TipOf(it->second.block, lane);
  return t == nullptr ? 0 : t->index();
}

void AutoBahn::ReceiveProposal(std::unique_ptr<Proposal> proposal) {
  if (!VerifyBlock(proposal->block())) return;
  bool with_parent = proposal->has_parent() &&
                     proposal->parent().hash() == proposal->block().parent_hash() &&
                     VerifyBlock(proposal->parent());
  auto p = std::make_shared<Proposal>(std::move(*proposal));
  Post([this, p, with_parent] {
    if (with_parent) StoreKnownBlock(p->parent());
    HandleProposal(p->block());
  });
}

// A certified block learned outside the voting path (parent of a view's
// first proposal, or a Status): known, never voted on.
void AutoBahn::StoreKnownBlock(const ConsensusBlock& b) {
  if (blocks_.count(b.hash())) return;
  BlockState& st = blocks_[b.hash()];
  st.block = b;
  st.known_only = true;
}

void AutoBahn::HandleProposal(const ConsensusBlock& b) {
  if (b.view() < view_) return;
  if (b.view() > view_) {
    future_blocks_[b.view()].push_back(b);
    return;
  }
  if (!in_view_ || b.proposer() != LeaderOf(b.view())) return;
  if (!consensus_started_) {
    consensus_started_ = true;
    view_start_ = Now();
  }

  auto key = std::make_pair(b.view(), b.height());
  auto seen = seen_heights_.find(key);
  if (seen != seen_heights_.end() && seen->second != b.hash()) {
    // Leader equivocation: two blocks at one height of the view.
    QuitView q;
    q.set_view(view_);
    *q.mutable_equivocation_a() = blocks_[seen->second].block;
    *q.mutable_equivocation_b() = b;
    Broadcast(MessageType::SyncHS_QuitView, q);
    QuitCurrentView();
    return;
  }
  auto existing = blocks_.find(b.hash());
  if (existing != blocks_.end() && !existing->second.known_only) return;
  seen_heights_[key] = b.hash();
  for (const Tip& t : b.tips()) LearnCertified(t.lane(), t.poa());
  BlockState& st = blocks_[b.hash()];
  st.block = b;
  st.known_only = false;
  st.receive_time = Now();
  pending_votes_.insert(std::make_pair(b.height(), b.hash()));
}

// Check (i): every tip is certified (verified on receipt), no lane regresses
// below its last committed position, and every tip is at least the highest
// certified tip of its lane the voter had held for Delta when the proposal
// arrived (freshness).
bool AutoBahn::ValidTipCut(const BlockState& st) {
  const ConsensusBlock& b = st.block;
  for (int j = 1; j <= n_; ++j) {
    const Tip* t = TipOf(b, j);
    int64_t tip = t == nullptr ? 0 : t->index();
    if (tip < lanes_[j].committed || tip < TipIndex(b.parent_hash(), j)) {
      return false;
    }
    int64_t held = 0;
    for (const auto& c : lanes_[j].certified_at) {
      if (c.second <= st.receive_time - params_.delta_us) held = c.first;
    }
    if (tip < held) return false;
  }
  return true;
}

// Checks (ii) and (iii); both delay the vote rather than reject the block.
bool AutoBahn::VoteWaitsOver(const BlockState& st) {
  const ConsensusBlock& b = st.block;
  if (!bof_) {
    // (ii) OL: the collection window closed for every transaction newly
    // referenced by the tip cut.
    int64_t now = Now();
    for (const Tip& t : b.tips()) {
      for (int64_t idx = TipIndex(b.parent_hash(), t.lane()) + 1; idx <= t.index();
           ++idx) {
        if (idx <= lanes_[t.lane()].committed) continue;
        const Car* car = FindCar(t.lane(), idx);
        if (car == nullptr) {
          RequestCar(t.lane(), idx);
          return false;
        }
        for (const OlCarItem& item : car->ol_items()) {
          auto it = txns_.find(item.txn_hash());
          if (it != txns_.end() &&
              now < it->second.first_seen + 2 * params_.delta_us) {
            return false;
          }
          if (!seen_.count(item.txn_hash())) return false;
        }
      }
    }
    return true;
  }
  // (iii) BOF: the voter's own delinquency timer expired for every claim.
  for (const Claim& c : b.claims()) {
    if (!delinquency_expired_.count({c.lane(), c.position()})) return false;
  }
  return true;
}

// Votes are cast in height order: the first vote of a view requires the
// parent certificate to rank at least as high as the lock; later votes must
// extend the block voted last.
void AutoBahn::ProcessPendingVotes() {
  if (!in_view_) return;
  for (auto it = pending_votes_.begin(); it != pending_votes_.end();) {
    auto bit = blocks_.find(it->second);
    if (bit == blocks_.end() || bit->second.block.view() != view_) {
      it = pending_votes_.erase(it);
      continue;
    }
    BlockState& st = bit->second;
    const ConsensusBlock& b = st.block;
    bool parent_ok = voted_tip_.empty()
                         ? !RankAbove(highest_qc_, b.parent_qc())
                         : b.parent_hash() == voted_tip_;
    bool parent_known = b.parent_hash().empty() || blocks_.count(b.parent_hash());
    if (!parent_ok || !parent_known) break;
    if (!st.tipcut_checked) {
      st.tipcut_checked = true;
      st.rejected = !ValidTipCut(st);
      if (st.rejected) {
        LOG(ERROR) << "abstaining on block " << b.height() << " of view "
                   << b.view() << ": invalid or stale tip cut";
      }
    }
    if (st.rejected) {
      it = pending_votes_.erase(it);
      continue;
    }
    if (!VoteWaitsOver(st)) break;
    CastVote(st);
    it = pending_votes_.erase(it);
  }
}

void AutoBahn::CastVote(BlockState& st) {
  const ConsensusBlock& b = st.block;
  st.voted = true;
  voted_tip_ = b.hash();
  if (b.proposer() != id_) {
    // Forwarding the proposal is what makes the 2 Delta commit rule safe.
    Proposal fwd;
    *fwd.mutable_block() = b;
    fwd.set_forwarder(id_);
    Broadcast(MessageType::SyncHS_Propose, fwd);
  }
  Vote v;
  v.set_view(b.view());
  v.set_height(b.height());
  v.set_block_hash(b.hash());
  v.set_sender_id(id_);
  auto sig = verifier_->SignMessage(VoteDigest(b.view(), b.height(), b.hash()));
  if (sig.ok()) *v.mutable_sign() = *sig;
  Broadcast(MessageType::SyncHS_Vote, v);
  HandleVote(v);
  int64_t now = Now();
  commit_timers_.push_back({now + 2 * params_.delta_us, b.hash(), b.view()});
  vote_times_.push_back(now);
}

void AutoBahn::ReceiveVote(std::unique_ptr<Vote> vote) {
  if (vote->sender_id() == id_ || !VerifyVote(*vote)) return;
  auto v = std::make_shared<Vote>(std::move(*vote));
  Post([this, v] { HandleVote(*v); });
}

void AutoBahn::HandleVote(const Vote& v) {
  auto& votes = votes_[v.block_hash()];
  if (!votes.emplace(v.sender_id(), v).second) return;
  if (static_cast<int>(votes.size()) != f_ + 1) return;
  QuorumCert qc;
  qc.set_view(v.view());
  qc.set_height(v.height());
  qc.set_block_hash(v.block_hash());
  for (const auto& e : votes) *qc.add_votes() = e.second;
  qcs_[v.block_hash()] = qc;
  if (RankAbove(qc, highest_qc_)) highest_qc_ = qc;
}

// Sync HotStuff commit rule: commit a block and its ancestors once its 2 Delta
// timer expires without detected equivocation and without leaving the view.
void AutoBahn::CheckCommitTimers() {
  int64_t now = Now();
  auto it = commit_timers_.begin();
  while (it != commit_timers_.end() && it->expire <= now) {
    if (it->view == view_ && in_view_) EnqueueCommitChain(it->hash);
    it = commit_timers_.erase(it);
  }
}

void AutoBahn::EnqueueCommitChain(const std::string& hash) {
  std::vector<std::string> chain;
  for (std::string h = hash; !h.empty() && h != committed_hash_;) {
    auto it = blocks_.find(h);
    if (it == blocks_.end() || it->second.committed || it->second.queued) break;
    chain.push_back(h);
    h = it->second.block.parent_hash();
  }
  for (auto it = chain.rbegin(); it != chain.rend(); ++it) {
    blocks_[*it].queued = true;
    commit_queue_.push_back(*it);
  }
}

// Committing a tip cut commits every Car of each lane up to its tip; the
// Cars are handed to the execution thread in commitment order (lane, then
// position).  Missing Cars are fetched first.
void AutoBahn::ProcessPendingCommits() {
  while (!commit_queue_.empty()) {
    BlockState& st = blocks_[commit_queue_.front()];
    const ConsensusBlock& b = st.block;
    bool complete = true;
    for (const Tip& t : b.tips()) {
      for (int64_t idx = lanes_[t.lane()].committed + 1; idx <= t.index(); ++idx) {
        if (FindCar(t.lane(), idx) == nullptr) {
          RequestCar(t.lane(), idx);
          complete = false;
        }
      }
    }
    if (!complete) return;

    CommittedBlock out;
    out.height = b.height();
    for (int j = 1; j <= n_; ++j) {
      const Tip* t = TipOf(b, j);
      if (t == nullptr) continue;
      Lane& lane = lanes_[j];
      for (int64_t idx = lane.committed + 1; idx <= t->index(); ++idx) {
        auto it = lane.cars.find(idx);
        Car car = it != lane.cars.end() ? it->second : lane.recent[idx];
        if (it != lane.cars.end()) lane.cars.erase(it);
        out.cars.push_back(car);
        lane.recent.emplace(idx, std::move(car));
        while (lane.recent.size() > kRecentCars) lane.recent.erase(lane.recent.begin());
      }
      if (t->index() > lane.committed) {
        lane.committed = t->index();
        lane.hashes.erase(lane.hashes.begin(), lane.hashes.lower_bound(lane.committed));
        lane.buffered.erase(lane.buffered.begin(),
                            lane.buffered.upper_bound(lane.committed));
        lane.queried.erase(lane.queried.begin(),
                           lane.queried.upper_bound(lane.committed));
        // A Car beyond the acknowledged prefix can now be skipped to.
        if (lane.acked < lane.committed) lane.acked = lane.committed;
      }
    }
    std::vector<int> claims;
    for (const Claim& c : b.claims()) {
      if (discarded_lanes_.insert(c.lane()).second) {
        claims.push_back(c.lane());
        LOG(ERROR) << "committed delinquency claim <" << c.lane() << ", "
                   << c.position() << "> in block " << b.height()
                   << ": lane " << c.lane() << " is ignored from now on";
      }
    }
    if (bof_) SettleBof(claims, &out);
    st.committed = true;
    committed_height_ = b.height();
    committed_hash_ = b.hash();
    commit_queue_.pop_front();
    {
      std::lock_guard<std::mutex> lk(exec_mutex_);
      exec_queue_.push_back(std::move(out));
    }
    exec_cv_.notify_one();
  }
}

// The leader proposes the next block once its previous one is certified,
// as soon as the tip cut advanced or a claim is due, and at least every Delta.
void AutoBahn::MaybePropose() {
  if (!in_view_ || LeaderOf(view_) != id_) return;
  int64_t now = Now();
  if (!leader_ready_) {
    if (view_ > 1 && now - view_start_ < 2 * params_.delta_us) return;
    // Extend the highest certificate reported by the Status messages.
    for (const auto& s : statuses_[view_]) {
      if (RankAbove(s.second.highest_qc(), highest_qc_) &&
          blocks_.count(s.second.highest_qc().block_hash())) {
        highest_qc_ = s.second.highest_qc();
      }
    }
    base_qc_ = highest_qc_;
    leader_ready_ = true;
  }

  std::string parent = base_qc_.block_hash();
  QuorumCert parent_qc = base_qc_;
  if (!last_proposed_.empty()) {
    auto qc = qcs_.find(last_proposed_);
    if (qc == qcs_.end()) {
      // Not certified yet: re-send it so late joiners can vote.
      if (now - last_propose_time_ >= params_.delta_us) {
        Proposal again;
        *again.mutable_block() = blocks_[last_proposed_].block;
        again.set_forwarder(id_);
        const std::string& p = again.block().parent_hash();
        if (again.block().height() == base_qc_.height() + 1 && !p.empty()) {
          *again.mutable_parent() = blocks_[p].block;
        }
        Broadcast(MessageType::SyncHS_Propose, again);
        last_propose_time_ = now;
      }
      return;
    }
    parent = last_proposed_;
    parent_qc = qc->second;
  }

  ConsensusBlock b;
  b.set_view(view_);
  b.set_height(parent_qc.height() + 1);
  b.set_parent_hash(parent);
  *b.mutable_parent_qc() = parent_qc;
  b.set_proposer(id_);
  bool advanced = false;
  for (int j = 1; j <= n_; ++j) {
    int64_t parent_tip = TipIndex(parent, j);
    const Poa& cert = lanes_[j].certified;
    Tip* t = nullptr;
    if (cert.index() > parent_tip) {
      t = b.add_tips();
      t->set_lane(j);
      t->set_index(cert.index());
      t->set_car_hash(cert.car_hash());
      *t->mutable_poa() = cert;
      advanced = true;
    } else if (parent_tip > 0) {
      *b.add_tips() = *TipOf(blocks_[parent].block, j);
    }
  }
  if (bof_) {
    for (const auto& target : claim_targets_) {
      int j = target.first;
      if (discarded_lanes_.count(j) || claimed_lanes_.count(j)) continue;
      if (!delinquency_expired_.count({j, target.second})) continue;
      Claim* c = b.add_claims();
      c->set_lane(j);
      c->set_position(target.second);
      claimed_lanes_.insert(j);
      advanced = true;
    }
  }
  if (!advanced && now - last_propose_time_ < params_.delta_us) return;

  b.set_propose_time(now);
  b.set_hash(BlockDigest(b));
  std::string tag;
  if (!tee_->SignBlock(b.view(), b.height(), b.hash(), &tag)) return;
  b.set_tee_tag(tag);
  last_proposed_ = b.hash();
  last_propose_time_ = now;

  Proposal p;
  *p.mutable_block() = b;
  p.set_forwarder(id_);
  if (b.height() == base_qc_.height() + 1 && !parent.empty()) {
    *p.mutable_parent() = blocks_[parent].block;
  }
  Broadcast(MessageType::SyncHS_Propose, p);
  HandleProposal(b);
}

// Blame (Sync HotStuff, budget widened to (3p+4)Delta): a replica blames the
// leader if fewer than p proposals triggered its vote within the budget.
void AutoBahn::CheckBlame() {
  int64_t now = Now();
  if (!consensus_started_ &&
      now - start_time_ >= kStartupGraceDeltas * params_.delta_us) {
    consensus_started_ = true;
    view_start_ = now;
  }
  if (!in_view_ || blamed_ || !consensus_started_) return;
  const int64_t budget = (3 * params_.blame_p + 4) * params_.delta_us;
  while (!vote_times_.empty() && vote_times_.front() < now - budget) {
    vote_times_.pop_front();
  }
  if (now - view_start_ < budget ||
      static_cast<int>(vote_times_.size()) >= params_.blame_p) {
    return;
  }
  Blame blame;
  blame.set_view(view_);
  blame.set_sender_id(id_);
  auto sig = verifier_->SignMessage(BlameDigest(view_));
  if (sig.ok()) *blame.mutable_sign() = *sig;
  blamed_ = true;
  LOG(ERROR) << "blaming leader " << LeaderOf(view_) << " of view " << view_;
  Broadcast(MessageType::SyncHS_Blame, blame);
  HandleBlame(blame);
}

bool AutoBahn::VerifyBlame(const Blame& b) {
  return b.sender_id() >= 1 && b.sender_id() <= n_ &&
         b.sign().node_id() == b.sender_id() &&
         verifier_->VerifyMessage(BlameDigest(b.view()), b.sign());
}

void AutoBahn::ReceiveBlame(std::unique_ptr<Blame> blame) {
  if (blame->sender_id() == id_ || !VerifyBlame(*blame)) return;
  auto b = std::make_shared<Blame>(std::move(*blame));
  Post([this, b] { HandleBlame(*b); });
}

// f+1 blames form the evidence that the view must be left.
void AutoBahn::HandleBlame(const Blame& blame) {
  if (blame.view() < view_) return;
  auto& blames = blames_[blame.view()];
  blames.emplace(blame.sender_id(), blame);
  if (blame.view() != view_ || !in_view_ ||
      static_cast<int>(blames.size()) < f_ + 1) {
    return;
  }
  QuitView q;
  q.set_view(view_);
  for (const auto& b : blames) *q.add_blames() = b.second;
  Broadcast(MessageType::SyncHS_QuitView, q);
  QuitCurrentView();
}

void AutoBahn::ReceiveQuitView(std::unique_ptr<QuitView> quit) {
  bool valid = false;
  std::set<int> signers;
  for (const Blame& b : quit->blames()) {
    if (b.view() == quit->view() && VerifyBlame(b)) signers.insert(b.sender_id());
  }
  valid = static_cast<int>(signers.size()) >= f_ + 1;
  const ConsensusBlock& a = quit->equivocation_a();
  const ConsensusBlock& c = quit->equivocation_b();
  if (!valid && a.view() == quit->view() && c.view() == quit->view() &&
      a.height() == c.height() && a.hash() != c.hash() &&
      a.proposer() == LeaderOf(quit->view()) && c.proposer() == a.proposer() &&
      VerifyBlock(a) && VerifyBlock(c)) {
    valid = true;
  }
  if (!valid) return;
  int64_t view = quit->view();
  auto q = std::make_shared<QuitView>(std::move(*quit));
  Post([this, q, view] {
    if (view < view_ || (view == view_ && !in_view_)) return;
    if (view > view_) {  // lagging replica: join the view being left
      view_ = view;
      in_view_ = true;
    }
    Broadcast(MessageType::SyncHS_QuitView, *q);  // relay the evidence
    QuitCurrentView();
  });
}

// Quit the view: stop voting, drop uncommitted timers of this view, and after
// 2 Delta report the lock to the next leader (EnterNextView).
void AutoBahn::QuitCurrentView() {
  if (!in_view_) return;
  LOG(ERROR) << "quitting view " << view_;
  in_view_ = false;
  quit_time_ = Now();
}

void AutoBahn::EnterNextView() {
  Status s;
  s.set_view(view_ + 1);
  s.set_sender_id(id_);
  *s.mutable_highest_qc() = highest_qc_;
  auto it = blocks_.find(highest_qc_.block_hash());
  if (it != blocks_.end()) *s.mutable_highest_block() = it->second.block;

  view_++;
  in_view_ = true;
  view_start_ = Now();
  blamed_ = false;
  leader_ready_ = false;
  vote_times_.clear();
  voted_tip_.clear();
  last_proposed_.clear();
  claimed_lanes_.clear();
  commit_timers_.clear();
  pending_votes_.clear();
  LOG(ERROR) << "entering view " << view_ << " (leader " << LeaderOf(view_) << ")";

  if (LeaderOf(view_) == id_) {
    statuses_[view_][id_] = s;
  } else {
    SendMessage(MessageType::SyncHS_Status, s, LeaderOf(view_));
  }
  auto fut = future_blocks_.find(view_);
  if (fut != future_blocks_.end()) {
    std::vector<ConsensusBlock> blocks = std::move(fut->second);
    future_blocks_.erase(fut);
    for (const auto& b : blocks) HandleProposal(b);
  }
  future_blocks_.erase(future_blocks_.begin(), future_blocks_.lower_bound(view_));
}

void AutoBahn::ReceiveStatus(std::unique_ptr<Status> status) {
  if (status->sender_id() < 1 || status->sender_id() > n_ ||
      !VerifyQc(status->highest_qc())) {
    return;
  }
  bool with_block = status->highest_qc().height() > 0 &&
                    status->highest_block().hash() == status->highest_qc().block_hash() &&
                    VerifyBlock(status->highest_block());
  auto s = std::make_shared<Status>(std::move(*status));
  Post([this, s, with_block] {
    if (with_block) StoreKnownBlock(s->highest_block());
    if (s->view() >= view_) statuses_[s->view()][s->sender_id()] = *s;
  });
}

// ============================================================
// BOF delinquency timers (Sec. 7.1)
// ============================================================

// Armed when the block fixing c_hat_j(t) commits; expires after
// D = 2X + 3 Delta = 5 Delta unless a certified Car of lane j at position
// c_hat_j(t) or beyond was received.  Expiry is final.
// Settlement is a function of the committed prefix only; running it here
// arms each delinquency timer when the block that fixes c_hat_j(t) commits.
void AutoBahn::SettleBof(const std::vector<int>& claims, CommittedBlock* out) {
  std::vector<BofCommittedCar> cars;
  for (const Car& car : out->cars) {
    BofCommittedCar c;
    c.lane = car.lane();
    c.index = car.index();
    c.w = car.frontier();
    c.items.assign(car.bof_items().begin(), car.bof_items().end());
    c.heartbeat.assign(car.bof_heartbeat().begin(), car.bof_heartbeat().end());
    c.m.assign(n_ + 1, BofCommittedCar::Announcement());
    for (int j = 1; j <= n_ && j <= car.m_size(); ++j) {
      c.m[j].present = car.m(j - 1).present();
      c.m[j].car = car.m(j - 1).car();
    }
    cars.push_back(std::move(c));
  }
  BofSettleResult res = bof_orderer_->Settle(cars, claims);
  ArmDelinquencyTimers(res.armed);
  claim_targets_ = bof_orderer_->ClaimTargets();
  out->bof_eligible = std::move(res.eligible);
  out->bof_pending = bof_orderer_->PendingSize();
}

void AutoBahn::ArmDelinquencyTimers(
    const std::vector<std::pair<int, int64_t>>& armed) {
  int64_t now = Now();
  for (const auto& a : armed) delinquency_armed_.emplace(a, now);
}

void AutoBahn::CheckDelinquencyTimers() {
  if (!bof_) return;
  const int64_t d = 5 * params_.delta_us;
  int64_t now = Now();
  for (auto it = delinquency_armed_.begin(); it != delinquency_armed_.end();) {
    const int lane = it->first.first;
    const int64_t position = it->first.second;
    if (lanes_[lane].certified.index() >= position) {
      it = delinquency_armed_.erase(it);  // the lane delivered: never expires
    } else if (now - it->second >= d) {
      delinquency_expired_.insert(it->first);
      it = delinquency_armed_.erase(it);
    } else {
      ++it;
    }
  }
}

// ============================================================
// Execution layer (Sec. 6-7)
// ============================================================

void AutoBahn::ExecutionLoop() {
  while (!IsStop()) {
    CommittedBlock block;
    {
      std::unique_lock<std::mutex> lk(exec_mutex_);
      exec_cv_.wait_for(lk, std::chrono::milliseconds(10),
                        [&] { return !exec_queue_.empty() || IsStop(); });
      if (exec_queue_.empty()) continue;
      block = std::move(exec_queue_.front());
      exec_queue_.pop_front();
    }
    Execute(block);
  }
}

void AutoBahn::Execute(const CommittedBlock& block) {
  int64_t now = Now();

  // Bodies of the newly committed transactions, and consensus latency over
  // those this replica received directly from a client.
  int new_txns = 0, samples = 0;
  int64_t latency_sum = 0;
  for (const Car& car : block.cars) {
    for (const Transaction& txn : car.txns()) {
      if (!exec_committed_.insert(txn.hash()).second) continue;
      ++new_txns;
      exec_bodies_.emplace(txn.hash(), txn);
      std::lock_guard<std::mutex> lk(create_mutex_);
      auto it = create_times_.find(txn.hash());
      if (it != create_times_.end()) {
        latency_sum += now - it->second;
        ++samples;
      }
    }
  }
  LOG(ERROR) << "slot_committed slot:" << block.height << " txns:" << new_txns
             << " consensus_latency_us:" << (samples ? latency_sum / samples : 0);

  std::vector<std::string> order;
  size_t pending = 0;
  if (!bof_) {
    std::vector<OlCommittedCar> cars;
    for (const Car& car : block.cars) {
      OlCommittedCar c;
      c.lane = car.lane();
      c.index = car.index();
      c.frontier = car.frontier();
      for (const OlCarItem& it : car.ol_items()) c.items.emplace_back(it.txn_hash(), it.key());
      cars.push_back(std::move(c));
    }
    order = ol_->Commit(cars);
    pending = ol_->PendingSize();
  } else if (!block.bof_eligible.empty()) {
    auto batches = BofOrderer::Order(block.bof_eligible, n_, bof_theta_);
    for (auto& batch : batches) {
      for (auto& id : batch) order.push_back(std::move(id));
    }
    pending = block.bof_pending;
  } else {
    pending = block.bof_pending;
  }
  // Enclave garbage collection (Sec. 5.1): executed transactions leave H.
  tee_->Forget(order);

  int64_t exec_now = Now();
  int64_t lat_sum = 0, lat_max = 0;
  int lat_samples = 0;
  for (const std::string& id : order) {
    auto body = exec_bodies_.find(id);
    if (body == exec_bodies_.end()) continue;
    Transaction& txn = body->second;
    txn.set_id(execute_id_++);
    Commit(txn);
    {
      std::lock_guard<std::mutex> lk(create_mutex_);
      auto it = create_times_.find(id);
      if (it != create_times_.end()) {
        int64_t lat = exec_now - it->second;
        lat_sum += lat;
        lat_max = std::max(lat_max, lat);
        ++lat_samples;
        create_times_.erase(it);
      }
    }
    exec_bodies_.erase(body);
  }
  // Rolling digest of the execution order, to check agreement across replicas.
  if (!order.empty()) {
    std::string chain = order_digest_;
    for (const std::string& id : order) chain += id;
    order_digest_ = TeeHost::Sha256(chain);
  }
  static const char kHex[] = "0123456789abcdef";
  std::string digest_hex;
  for (int i = 0; i < 8 && i < static_cast<int>(order_digest_.size()); ++i) {
    digest_hex += kHex[(order_digest_[i] >> 4) & 0xf];
    digest_hex += kHex[order_digest_[i] & 0xf];
  }
  LOG(ERROR) << "executed_batch slot:" << block.height << " count:" << order.size()
             << " avg_latency_us:" << (lat_samples ? lat_sum / lat_samples : 0)
             << " max_latency_us:" << lat_max << " pending:" << pending
             << " order_digest:" << digest_hex;
  if (!order.empty()) {
    global_stats_->ConsumeTransactions(static_cast<int>(order.size()));
  }
}

}  // namespace autobahn
}  // namespace resdb
