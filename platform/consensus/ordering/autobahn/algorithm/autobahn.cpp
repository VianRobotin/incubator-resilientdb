#include "platform/consensus/ordering/autobahn/algorithm/autobahn.h"

#include <glog/logging.h>

#include <pthread.h>

#include <cmath>
#include <cstdio>

#include <algorithm>

#include "common/crypto/signature_verifier.h"
#include "common/utils/utils.h"
#include "platform/consensus/ordering/autobahn/tee/aes_cmac.h"
#include "platform/networkstrate/traffic.h"

namespace resdb {
namespace autobahn {

namespace {

constexpr size_t kEchoFlushSize = 512;
constexpr int64_t kEchoFlushUs = 2000;
// Client transactions are shed once a shard's input queue holds this many.
constexpr size_t kMaxInbox = 20000;
// Bodies (and the seen-set entry) are kept this long after execution, so
// that lagging replicas can still fetch them.
constexpr int64_t kBodyRetentionUs = 2000000;
constexpr size_t kBodyQueryMax = 1000;
constexpr int kSenderThreads = 4;
// Admission control: adjusted every period; outgoing backlog watermarks.
constexpr int64_t kAdmissionPeriodUs = 10000;   // fast path
constexpr int kModelPeriods = 10;                 // model: every 100 ms
constexpr double kMinAdmitRate = 1000;            // tx/s
constexpr double kInitialAdmitRate = 5000;        // tx/s, before any estimate
constexpr double kLinkBudget = 0.75;              // share of line rate
// Approximate wire sizes (bytes) of a client request, an echo and a Car
// entry, for the capacity prior.
constexpr double kClientBytes = 340;
constexpr double kEchoBytes = 260;
constexpr double kCarItemBytes = 45;
constexpr int64_t kBacklogHigh = 1 << 20;
constexpr int64_t kBacklogLow = 256 << 10;
constexpr size_t kVerifiedCache = 1 << 16;
constexpr size_t kRecentCars = 256;  // committed Cars kept to answer queries
// Startup: the first block is proposed only once every replica's lane has
// delivered a Car (all replicas are running, so none misses the first
// blocks and is left behind); blame timers start at the first proposal, or
// this many Delta after that point if none arrives.
constexpr int64_t kStartupGraceDeltas = 40;

int64_t Now() { return static_cast<int64_t>(GetCurrentTime()); }

void PutI64(std::string* s, int64_t v) {
  for (int i = 0; i < 8; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}
void PutI32(std::string* s, int32_t v) {
  for (int i = 0; i < 4; ++i) s->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

std::string ZeroHash() { return std::string(32, '\0'); }

// Bytes this replica's node has read and written so far.  The co-located
// client's outgoing traffic is not visible here; it is approximated by the
// client traffic this replica receives (clients broadcast to every replica).
bool ReadTraffic(int64_t* rx, int64_t* tx) {
  *rx = NetworkTraffic::RxBytes().load(std::memory_order_relaxed);
  *tx = NetworkTraffic::TxBytes().load(std::memory_order_relaxed) +
        NetworkTraffic::ClientRxBytes().load(std::memory_order_relaxed);
  return true;
}


}  // namespace

AutoBahn::AutoBahn(int id, int f, int total_num, int block_size,
                   SignatureVerifier* verifier, const Params& params)
    : ProtocolBase(id, f, total_num),
      n_(total_num),
      block_size_(block_size),
      params_(params),
      bof_(params.batch_order_fairness),
      verifier_(verifier),
      lanes_(total_num + 1),
      heard_(total_num + 1, false) {
  global_stats_ = Stats::GetGlobalStats(5);
  tee_ = std::make_unique<TeeHost>();
  CHECK(tee_->Initialize(params_.enclave_path, id_, n_, f_, bof_,
                         params_.group_seed))
      << "Pearl requires its enclave (" << params_.enclave_path << ")";
  if (bof_) {
    bof_orderer_ = std::make_unique<BofOrderer>(n_, f_, params_.bof_gamma);
    bof_theta_ = bof_orderer_->Theta();
    bof_hold_ = std::make_unique<BofHoldBack>(n_, bof_theta_);
  } else {
    ol_ = std::make_unique<OlOrderer>(n_, f_);
  }
  for (int k = 0; k < std::max(1, params_.dissemination_threads); ++k) {
    shards_.push_back(std::make_unique<Shard>());
  }
  std::string admission_key = tee_->AdmissionKey();
  CHECK_EQ(admission_key.size(), 16u) << "enclave gave no admission key";
  admission_mac_ = std::make_unique<pearl::AesCmac>();
  admission_mac_->SetKey(reinterpret_cast<const uint8_t*>(admission_key.data()));
  own_rate_ = kInitialAdmitRate;
  reported_rate_.reset(new std::atomic<int64_t>[n_ + 1]);
  reported_at_.reset(new std::atomic<int64_t>[n_ + 1]);
  for (int j = 0; j <= n_; ++j) {
    reported_rate_[j] = 0;
    reported_at_[j] = 0;
  }
  capacity_ = 1e9;
  last_car_hash_ = ZeroHash();
  int64_t now = Now();
  last_seal_ = last_activity_ = view_start_ = start_time_ = now;

  LOG(ERROR) << "Pearl-" << (bof_ ? "BOF" : "OL") << " replica " << id_
             << " n=" << n_ << " f=" << f_ << " block_size=" << block_size_
             << " Delta=" << params_.delta_us << "us shards=" << shards_.size()
             << (bof_ ? " theta=" + std::to_string(bof_orderer_->Theta()) : "");
}

AutoBahn::~AutoBahn() {
  Stop();
  event_cv_.notify_all();
  exec_cv_.notify_all();
  send_cv_.notify_all();
  for (auto& t : send_threads_) {
    if (t.joinable()) t.join();
  }
  for (auto& s : shards_) s->in_cv.notify_all();
  if (protocol_thread_.joinable()) protocol_thread_.join();
  if (execution_thread_.joinable()) execution_thread_.join();
  settle_cv_.notify_all();
  if (settle_thread_.joinable()) settle_thread_.join();
  if (admission_thread_.joinable()) admission_thread_.join();
  for (auto& s : shards_) {
    if (s->thread.joinable()) s->thread.join();
  }
}

void AutoBahn::Start() {
  protocol_thread_ = std::thread(&AutoBahn::ProtocolLoop, this);
  pthread_setname_np(protocol_thread_.native_handle(), "pearl-protocol");
  execution_thread_ = std::thread(&AutoBahn::ExecutionLoop, this);
  pthread_setname_np(execution_thread_.native_handle(), "pearl-exec");
  if (bof_) {
    settle_thread_ = std::thread(&AutoBahn::SettleLoop, this);
    pthread_setname_np(settle_thread_.native_handle(), "pearl-settle");
  }
  admission_thread_ = std::thread(&AutoBahn::AdmissionLoop, this);
  pthread_setname_np(admission_thread_.native_handle(), "pearl-admit");
  for (int k = 0; k < kSenderThreads; ++k) {
    send_threads_.emplace_back(&AutoBahn::SendLoop, this);
    pthread_setname_np(send_threads_.back().native_handle(), "pearl-send");
  }
  for (size_t k = 0; k < shards_.size(); ++k) {
    shards_[k]->thread = std::thread(&AutoBahn::ShardLoop, this, shards_[k].get());
    pthread_setname_np(shards_[k]->thread.native_handle(),
                       ("pearl-shard-" + std::to_string(k)).c_str());
  }
}

// ============================================================
// Sending
// ============================================================

int AutoBahn::Broadcast(int msg_type, const google::protobuf::Message& msg) {
  return SendMessage(msg_type, msg, 0);
}

int AutoBahn::SendMessage(int msg_type, const google::protobuf::Message& msg,
                          int node_id) {
  Outgoing out;
  out.type = msg_type;
  out.target = node_id;
  out.queued = Now();
  out.msg.reset(msg.New());
  out.msg->CopyFrom(msg);
  {
    std::lock_guard<std::mutex> lk(send_mu_);
    send_queue_.push_back(std::move(out));
  }
  send_cv_.notify_one();
  return 0;
}

void AutoBahn::SendLoop() {
  while (true) {
    Outgoing out;
    {
      std::unique_lock<std::mutex> lk(send_mu_);
      send_cv_.wait_for(lk, std::chrono::milliseconds(10),
                        [&] { return !send_queue_.empty() || IsStop(); });
      if (send_queue_.empty()) {
        if (IsStop()) return;
        continue;
      }
      out = std::move(send_queue_.front());
      send_queue_.pop_front();
      int64_t wait = Now() - out.queued;
      static int64_t last_report = 0;
      if (wait > params_.delta_us / 2 && Now() - last_report > 1000000) {
        last_report = Now();
        LOG(ERROR) << "send queue: " << send_queue_.size() << " waiting, oldest "
                   << wait << "us";
      }
    }
    if (out.prepare) out.prepare(out.msg.get());
    if (out.target == 0) {
      ProtocolBase::Broadcast(out.type, *out.msg);
    } else {
      ProtocolBase::SendMessage(out.type, *out.msg, out.target);
    }
  }
}

bool AutoBahn::CheckVerified(const std::string& key) {
  std::lock_guard<std::mutex> lk(verified_mu_);
  return verified_.contains(key);
}

void AutoBahn::MarkVerified(const std::string& key) {
  std::lock_guard<std::mutex> lk(verified_mu_);
  if (!verified_.insert(key).second) return;
  verified_order_.push_back(key);
  if (verified_order_.size() > kVerifiedCache) {
    verified_.erase(verified_order_.front());
    verified_order_.pop_front();
  }
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
    int64_t start = Now();
    for (auto& fn : batch) fn();
    int64_t events = Now() - start;
    if (events > params_.delta_us / 2) {
      LOG(ERROR) << "protocol thread: " << batch.size() << " events took " << events << "us";
    }
    Tick();
  }
}

void AutoBahn::Tick() {
  int64_t now = Now();
  int64_t mark = now;
  auto step = [&](const char* name) {
    int64_t t = Now();
    if (t - mark > params_.delta_us / 2) LOG(ERROR) << "protocol thread: " << name << " took " << (t - mark) << "us";
    mark = t;
  };
  step("admission");
  MaybeHeartbeat();
  MaybeSealCar();
  step("seal");
  AdvanceLanes();
  RequestMissingCars();
  step("lanes");
  CheckDelinquencyTimers();
  ProcessPendingVotes();
  step("votes");
  CheckCommitTimers();
  ProcessPendingCommits();
  step("commits");
  CheckBlame();
  if (!in_view_ && now - quit_time_ >= 2 * params_.delta_us) EnterNextView();
  MaybePropose();
  step("propose");
}

// ============================================================
// Dissemination layer (Sec. 5.2, Alg. 1)
// ============================================================

AutoBahn::Shard& AutoBahn::ShardOf(const TxnId& id) {
  uint64_t v;
  std::memcpy(&v, id.b.data() + 8, sizeof(v));  // bytes the hash maps skip
  return *shards_[v % shards_.size()];
}

void AutoBahn::Route(std::vector<Incoming> items) {
  if (items.empty()) return;
  if (shards_.size() == 1) {
    Shard& s = *shards_[0];
    {
      std::lock_guard<std::mutex> lk(s.in_mu);
      for (auto& in : items) s.inbox.push_back(std::move(in));
      s.inbox_size = s.inbox.size();
    }
    s.in_cv.notify_one();
    return;
  }
  std::vector<std::vector<Incoming>> parts(shards_.size());
  for (auto& in : items) {
    uint64_t v;
    std::memcpy(&v, in.id.b.data() + 8, sizeof(v));
    parts[v % shards_.size()].push_back(std::move(in));
  }
  for (size_t k = 0; k < parts.size(); ++k) {
    if (parts[k].empty()) continue;
    Shard& s = *shards_[k];
    {
      std::lock_guard<std::mutex> lk(s.in_mu);
      for (auto& in : parts[k]) s.inbox.push_back(std::move(in));
      s.inbox_size = s.inbox.size();
    }
    s.in_cv.notify_one();
  }
}

// A transaction from a client.
//
// Admission control: beyond capacity, queues (outgoing connections, shard
// inputs) would grow until messages miss the synchrony bound Delta.  A
// replica therefore admits a client transaction only if its rank lies below
// the admission threshold (see AdjustAdmission).  The rank is a MAC of id(t)
// under a key derived from the enclaves' committee key: identical at every
// replica, so correct replicas admit the same transactions and a transaction
// one sheds is not still disseminated by the others, and unpredictable to
// clients, who cannot craft transactions that are never shed.  Echoes and
// fetched bodies are never shed.
bool AutoBahn::ReceiveTransaction(std::string data) {
  Incoming in;
  in.id = TxnId::From(SignatureVerifier::CalculateHash(data));
  client_arrivals_.fetch_add(1, std::memory_order_relaxed);
  uint8_t mac[pearl::AesCmac::kTagLen];
  admission_mac_->Mac(in.id.b.data(), static_cast<uint32_t>(in.id.b.size()), mac);
  uint32_t rank;
  std::memcpy(&rank, mac, sizeof(rank));
  if (static_cast<int>(rank % kAdmitAll) >= admit_.load(std::memory_order_relaxed) ||
      ShardOf(in.id).inbox_size.load(std::memory_order_relaxed) > kMaxInbox) {
    int64_t d = ++dropped_;
    if (d % 100000 == 1) {
      LOG(ERROR) << "overload: shed " << d << " client transactions (admitting "
                 << admit_.load() / (kAdmitAll / 100) << "%)";
    }
    return false;
  }
  in.data = std::make_shared<const std::string>(std::move(data));
  in.client_time = Now();
  std::vector<Incoming> v;
  v.push_back(std::move(in));
  Route(std::move(v));
  return true;
}

// OnReceiveOLEcho / OnReceiveBOFEcho: TEE.Verify each echo (stateless, on
// the network thread), then hand it to the transaction's shard.
void AutoBahn::ReceiveEchoBatch(std::unique_ptr<EchoBatch> batch) {
  const int sender = batch->sender_id();
  if (sender == id_ || sender < 1 || sender > n_) return;
  const int m = batch->echoes_size();
  std::vector<Incoming> items(m);
  std::vector<TeeHost::VerifyItem> verify(m);
  for (int i = 0; i < m; ++i) {
    Echo* e = batch->mutable_echoes(i);
    std::string hash = SignatureVerifier::CalculateHash(e->data());
    items[i].id = TxnId::From(hash);
    verify[i].msg = bof_ ? TeeHost::BofPlacementMsg(sender, hash, e->value())
                         : TeeHost::OlTimestampMsg(sender, hash, e->value());
    verify[i].tag = e->tee_tag();
  }
  std::vector<bool> ok = tee_->VerifyBatch(verify);
  std::vector<Incoming> good;
  good.reserve(m);
  for (int i = 0; i < m; ++i) {
    if (!ok[i]) continue;
    Echo* e = batch->mutable_echoes(i);
    Incoming& in = items[i];
    in.data = std::make_shared<const std::string>(std::move(*e->mutable_data()));
    in.signer = sender;
    in.value = e->value();
    std::memcpy(in.tag.data(), e->tee_tag().data(), TeeHost::kTagLen);
    good.push_back(std::move(in));
  }
  Route(std::move(good));
}

void AutoBahn::ShardLoop(Shard* shard) {
  Shard& s = *shard;
  while (!IsStop() && !network_ready_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  std::vector<Incoming> batch;
  std::vector<TxnId> gc;
  while (!IsStop()) {
    {
      std::unique_lock<std::mutex> lk(s.in_mu);
      s.in_cv.wait_for(lk, std::chrono::milliseconds(1),
                       [&] { return !s.inbox.empty() || IsStop(); });
      batch.swap(s.inbox);
      gc.swap(s.gc_inbox);
      s.inbox_size = 0;
    }
    int64_t now = Now();
    if (!gc.empty()) {
      s.gc_pending.emplace_back(now + kBodyRetentionUs, std::move(gc));
      gc.clear();
    }
    {
      std::lock_guard<std::mutex> lk(s.mu);
      if (!batch.empty()) ShardFirstReceive(s, batch);
      ShardCloseWindows(s);
      while (!s.gc_pending.empty() && s.gc_pending.front().first <= now) {
        for (const TxnId& id : s.gc_pending.front().second) {
          s.seen.erase(id);
          s.bodies.erase(id);
        }
        s.gc_pending.pop_front();
      }
    }
    batch.clear();
    FlushEchoes(s, false);
  }
}

// OnFirstReceive (Alg. 1) for a batch: the enclave attests every
// transaction this replica sees for the first time (OL timestamp, BOF
// placement), which is then echoed with that evidence; echoes received from
// other replicas add their evidence to the collection window.
void AutoBahn::ShardFirstReceive(Shard& s, std::vector<Incoming>& batch) {
  const int64_t now = Now();
  std::string ids;
  std::vector<Incoming*> fresh;
  for (Incoming& in : batch) {
    if (in.client_time > 0) {
      auto b = s.bodies.find(in.id);
      if (b != s.bodies.end()) b->second.from_client = true;
    }
    if (!s.seen.insert(in.id).second) continue;
    ids.append(reinterpret_cast<const char*>(in.id.b.data()), 32);
    fresh.push_back(&in);
  }
  if (!fresh.empty()) {
    last_activity_.store(now, std::memory_order_relaxed);
    std::vector<int64_t> values;
    std::vector<uint8_t> ok;
    std::string tags;
    tee_->AttestBatch(reinterpret_cast<const uint8_t*>(ids.data()),
                      static_cast<uint32_t>(fresh.size()), &values, &tags, &ok);
    int placed = 0;
    for (size_t i = 0; i < fresh.size(); ++i) {
      Incoming& in = *fresh[i];
      if (!ok[i]) continue;  // already in the enclave's history H
      ++placed;
      Record& rec = s.open[in.id];
      rec.first_seen = now;
      Evidence own;
      own.signer = id_;
      own.value = values[i];
      std::memcpy(own.tag.data(), tags.data() + i * TeeHost::kTagLen,
                  TeeHost::kTagLen);
      rec.evidence.push_back(own);
      s.bodies[in.id] = Body{in.data, in.client_time > 0 ? in.client_time : now,
                             in.client_time > 0};
      s.windows.emplace_back(now + 2 * params_.delta_us, in.id);
      Echo* e = s.echo_out.add_echoes();
      e->set_data(*in.data);
      e->set_value(values[i]);
      e->set_tee_tag(tags.data() + i * TeeHost::kTagLen, TeeHost::kTagLen);
      if (s.echo_since == 0) s.echo_since = now;
    }
    if (bof_ && placed > 0) bof_open_count_ += placed;
  }
  for (const Incoming& in : batch) {
    if (in.signer == 0) continue;
    auto it = s.open.find(in.id);
    if (it == s.open.end()) continue;  // window already closed
    bool dup = false;
    for (const Evidence& e : it->second.evidence) dup |= e.signer == in.signer;
    if (dup) continue;
    Evidence e;
    e.signer = in.signer;
    e.value = in.value;
    e.tag = in.tag;
    it->second.evidence.push_back(e);
  }
}

// AfterCollectionWindow (Alg. 1): 2 Delta after first reception.
//   OL:  the enclave reduces the collected timestamps to K_r(t), which is
//        placed in the next Car.
//   BOF: the enclave records the collected placement announcements, closing
//        t's window and advancing (w_r, M_r).
void AutoBahn::ShardCloseWindows(Shard& s) {
  const int64_t now = Now();
  std::vector<TxnId> due;
  for (auto* q : {&s.windows, &s.retry}) {
    while (!q->empty() && q->front().first <= now) {
      due.push_back(q->front().second);
      q->pop_front();
    }
  }
  if (due.empty()) return;
  std::string collected;
  std::vector<TxnId> ids;
  ids.reserve(due.size());
  for (const TxnId& id : due) {
    auto it = s.open.find(id);
    if (it == s.open.end()) continue;
    const auto& ev = it->second.evidence;
    TeeHost::AppendCollected(&collected, id.b.data(), static_cast<uint32_t>(ev.size()));
    for (const Evidence& e : ev) {
      TeeHost::AppendRecord(&collected, e.signer, e.value, e.tag.data());
    }
    ids.push_back(id);
  }
  std::vector<int64_t> keys;
  std::vector<uint8_t> ok;
  tee_->CloseWindowsBatch(collected, static_cast<uint32_t>(ids.size()), &keys, &ok);
  std::vector<TeeHost::OlItem> keyed;
  for (size_t i = 0; i < ids.size(); ++i) {
    if (!ok[i]) {
      if (bof_) {
        LOG(ERROR) << "BOF: enclave refused announcements for a transaction";
        s.open.erase(ids[i]);
      } else {
        // Fewer than f+1 genuine timestamps so far: not reachable when the
        // synchrony bound holds.  Keep collecting and retry; the completion
        // frontier cannot pass t meanwhile.
        s.retry.emplace_back(now + params_.delta_us, ids[i]);
      }
      continue;
    }
    s.open.erase(ids[i]);
    if (!bof_) keyed.push_back({ids[i].Str(), keys[i]});
  }
  if (!keyed.empty()) {
    std::lock_guard<std::mutex> lk(ol_mu_);
    for (auto& k : keyed) ol_open_.push_back(std::move(k));
  }
}

// Echoes are sent in batches; each echo is still an individual
// retransmission of t with this replica's evidence.
void AutoBahn::FlushEchoes(Shard& s, bool force) {
  if (s.echo_out.echoes_size() == 0) return;
  if (!force && s.echo_out.echoes_size() < static_cast<int>(kEchoFlushSize) &&
      Now() - s.echo_since < kEchoFlushUs) {
    return;
  }
  s.echo_out.set_sender_id(id_);
  echo_bytes_ += static_cast<int64_t>(s.echo_out.ByteSizeLong()) * (n_ - 1);
  echoed_txns_ += s.echo_out.echoes_size();
  if (bulk_send_) {
    // Sent from the shard thread on the bulk connections, so that echoes
    // never queue ahead of consensus and lane messages.
    bulk_send_(MessageType::Echo_Batch, s.echo_out);
  } else {
    Broadcast(MessageType::Echo_Batch, s.echo_out);
  }
  s.echo_out.Clear();
  s.echo_since = 0;
}

std::vector<TxnId> AutoBahn::CarTxnIds(const Car& car) const {
  std::vector<TxnId> ids;
  if (!bof_) {
    ids.reserve(car.ol_items_size());
    for (const OlCarItem& it : car.ol_items()) ids.push_back(TxnId::From(it.txn_hash()));
  } else {
    ids.reserve(car.bof_items_size());
    for (int i = 0; i < car.bof_items_size(); ++i) {
      if (!car.bof_heartbeat(i)) ids.push_back(TxnId::From(car.bof_items(i)));
    }
  }
  return ids;
}

std::vector<TxnId> AutoBahn::MissingBodies(const std::vector<TxnId>& ids) {
  std::vector<std::vector<const TxnId*>> parts(shards_.size());
  for (const TxnId& id : ids) {
    uint64_t v;
    std::memcpy(&v, id.b.data() + 8, sizeof(v));
    parts[v % shards_.size()].push_back(&id);
  }
  std::vector<TxnId> missing;
  for (size_t k = 0; k < parts.size(); ++k) {
    if (parts[k].empty()) continue;
    Shard& s = *shards_[k];
    std::lock_guard<std::mutex> lk(s.mu);
    for (const TxnId* id : parts[k]) {
      if (!s.bodies.contains(*id)) missing.push_back(*id);
    }
  }
  return missing;
}

// Check (ii): this replica's own collection window closed for every given
// transaction, i.e. it received each at least 2 Delta ago.
bool AutoBahn::WindowsClosed(const std::vector<TxnId>& ids) {
  const int64_t now = Now();
  std::vector<std::vector<const TxnId*>> parts(shards_.size());
  for (const TxnId& id : ids) {
    uint64_t v;
    std::memcpy(&v, id.b.data() + 8, sizeof(v));
    parts[v % shards_.size()].push_back(&id);
  }
  for (size_t k = 0; k < parts.size(); ++k) {
    if (parts[k].empty()) continue;
    Shard& s = *shards_[k];
    std::lock_guard<std::mutex> lk(s.mu);
    for (const TxnId* id : parts[k]) {
      if (!s.seen.contains(*id)) return false;
      auto it = s.open.find(*id);
      if (it != s.open.end() && now < it->second.first_seen + 2 * params_.delta_us) {
        return false;
      }
    }
  }
  return true;
}

// Fetch transaction bodies referenced by a Car: from its lane's owner
// (target > 0) or from every replica (target == 0).
void AutoBahn::RequestBodies(const std::vector<TxnId>& ids, int target) {
  for (size_t i = 0; i < ids.size(); i += kBodyQueryMax) {
    BodyQuery q;
    q.set_requester(id_);
    for (size_t j = i; j < ids.size() && j < i + kBodyQueryMax; ++j) {
      q.add_ids(ids[j].Str());
    }
    if (target > 0) {
      SendMessage(MessageType::Body_Query, q, target);
    } else {
      Broadcast(MessageType::Body_Query, q);
    }
  }
}

void AutoBahn::ReceiveBodyQuery(std::unique_ptr<BodyQuery> query) {
  if (query->requester() < 1 || query->requester() > n_ ||
      query->requester() == id_) {
    return;
  }
  BodyReply reply;
  reply.set_sender_id(id_);
  for (const std::string& raw : query->ids()) {
    TxnId id = TxnId::From(raw);
    Shard& s = ShardOf(id);
    std::lock_guard<std::mutex> lk(s.mu);
    auto it = s.bodies.find(id);
    if (it != s.bodies.end()) reply.add_data(*it->second.data);
  }
  if (reply.data_size() > 0) {
    SendMessage(MessageType::Body_Reply, reply, query->requester());
  }
}

// A body received in reply to a fetch is a transaction received "from
// another replica in a Car" (Sec. 5.2): it goes through OnFirstReceive.
void AutoBahn::ReceiveBodyReply(std::unique_ptr<BodyReply> reply) {
  std::vector<Incoming> items;
  items.reserve(reply->data_size());
  for (std::string& data : *reply->mutable_data()) {
    Incoming in;
    in.id = TxnId::From(SignatureVerifier::CalculateHash(data));
    in.data = std::make_shared<const std::string>(std::move(data));
    items.push_back(std::move(in));
  }
  Route(std::move(items));
}


// Runs on its own thread: reading the outgoing backlog takes the network
// layer's locks, which must never stall the protocol thread.
void AutoBahn::AdmissionLoop() {
  while (!IsStop()) {
    std::this_thread::sleep_for(std::chrono::microseconds(kAdmissionPeriodUs));
    if (network_ready_) AdjustAdmission();
  }
}

// Keeps the link below saturation: a saturated 1 Gbps port drops packets,
// and TCP's retransmission timeout (>= 200 ms) then delays every message,
// consensus included, far beyond Delta.
//
// Each replica controls an admitted rate R (transactions/s):
//  - every 100 ms it estimates its capacity, the transactions per second it
//    can disseminate while its link stays at 75% of line rate: the budget
//    (75% of the link minus the other traffic it measures) over the bytes
//    it sends per transaction, capped by a prior computed from message
//    sizes.  R drops to that capacity at once, and grows towards it by at
//    most 5% per period, and only while client demand exceeds R (slow start,
//    so a load burst cannot saturate the link before the estimate catches
//    up);
//  - every 10 ms, a backed-up queue (bytes not yet on the wire, shard
//    inputs) halves R.
// R is reported in the replica's Cars; every replica uses the (f+1)-th
// smallest recent R, so correct replicas agree on it and f faulty replicas
// cannot starve admission.  The admitted fraction is that rate over the
// client arrival rate, which all replicas observe alike (clients broadcast),
// so correct replicas admit the same transactions.
void AutoBahn::AdjustAdmission() {
  const double link = static_cast<double>(params_.link_bytes_per_sec);
  const double tick = kAdmissionPeriodUs / 1e6;
  const int64_t arrived = client_arrivals_.exchange(0);
  window_arrivals_ += arrived;
  arrival_rate_ = 0.5 * arrival_rate_ + 0.5 * (arrived / tick);

  int64_t backlog = outbound_backlog_ ? outbound_backlog_() : 0;
  size_t inbox = 0;
  for (const auto& s : shards_) inbox = std::max(inbox, s->inbox_size.load());
  if (backlog > kBacklogHigh || inbox > kMaxInbox / 4) {
    own_rate_ = std::max(kMinAdmitRate, own_rate_.load() * 0.5);
  }

  if (++admission_ticks_ >= kModelPeriods) {
    admission_ticks_ = 0;
    const double secs = kModelPeriods * tick;
    const double demand = window_arrivals_ / secs;
    window_arrivals_ = 0;
    const double echo_rate = echo_bytes_.exchange(0) / secs;
    const double echoed = echoed_txns_.exchange(0) / secs;
    int64_t rx = 0, tx = 0;
    ReadTraffic(&rx, &tx);
    double used = 0;
    if (nic_rx_ >= 0) used = std::max(rx - nic_rx_, tx - nic_tx_) / secs;
    nic_rx_ = rx;
    nic_tx_ = tx;
    // Prior: each admitted transaction costs about (n-1) echoes and Car
    // entries in each direction, each offered one a client copy; the
    // measured estimate may only lower it.
    const double prior =
        std::max(0.0, kLinkBudget * link - demand * kClientBytes) /
        ((n_ - 1) * (kEchoBytes + kCarItemBytes));
    double estimate = prior;
    if (echoed > 0) {
      double per_txn = echo_rate / echoed * (kEchoBytes + kCarItemBytes) / kEchoBytes;
      double budget = std::max(0.0, kLinkBudget * link - std::max(0.0, used - echo_rate));
      estimate = std::min(prior, budget / per_txn);
    }
    capacity_ = std::min(prior, 0.5 * capacity_ + 0.5 * estimate);
    double rate = own_rate_.load();
    if (rate > capacity_) {
      rate = capacity_;
    } else if (demand > rate) {
      rate = std::min(capacity_, rate * 1.05 + 100);
    }
    own_rate_ = std::max(kMinAdmitRate, rate);
    link_used_ = used;
  }

  const int64_t now = Now();
  std::vector<double> rates = {own_rate_.load()};
  for (int j = 1; j <= n_; ++j) {
    int64_t at = reported_at_[j].load(std::memory_order_relaxed);
    if (j != id_ && at > 0 && now - at <= 2 * params_.delta_us) {
      rates.push_back(static_cast<double>(reported_rate_[j].load(std::memory_order_relaxed)));
    }
  }
  std::sort(rates.begin(), rates.end());
  double effective = static_cast<int>(rates.size()) > f_ ? rates[f_] : rates.front();
  int admit = arrival_rate_ <= effective
                  ? kAdmitAll
                  : static_cast<int>(kAdmitAll * effective / arrival_rate_);
  admit_ = std::max(1, admit);
  static int64_t last_report = 0;
  if (now - last_report > 5000000) {
    last_report = now;
    LOG(ERROR) << "admission: " << admit_ / (kAdmitAll / 100) << "% of "
               << static_cast<int64_t>(arrival_rate_) << " tx/s (rate "
               << static_cast<int64_t>(effective) << ", own "
               << static_cast<int64_t>(own_rate_.load()) << ", capacity "
               << static_cast<int64_t>(capacity_) << ", link "
               << static_cast<int64_t>(link_used_ / 1000000) << " MB/s, backlog "
               << backlog << " B)";
  }
}

// Heartbeat (Sec. 5.2): when no transaction has arrived for a configurable
// period, have the enclave issue a fresh indicator so frontiers keep moving.
void AutoBahn::MaybeHeartbeat() {
  int64_t now = Now();
  if (now - last_activity_.load(std::memory_order_relaxed) < params_.heartbeat_us ||
      now - last_heartbeat_ < params_.heartbeat_us) {
    return;
  }
  std::string id;
  int64_t value = 0;
  if (!tee_->Heartbeat(&id, &value)) return;
  last_heartbeat_ = now;
  if (bof_) {
    ++bof_open_count_;
  } else {
    heartbeat_since_seal_ = true;
  }
}

// A Car is sealed once full (checked on insertion) or once X = Delta has
// elapsed since the previous seal.
void AutoBahn::MaybeSealCar() {
  const bool timer = Now() - last_seal_ >= params_.delta_us;
  if (bof_) {
    int open = bof_open_count_.load();
    if (open >= block_size_ || (open > 0 && timer)) SealCar();
    return;
  }
  size_t open;
  {
    std::lock_guard<std::mutex> lk(ol_mu_);
    open = ol_open_.size();
  }
  // A backlog is sealed one full Car at a time.
  for (; open >= static_cast<size_t>(block_size_); open -= block_size_) SealCar();
  if ((open > 0 || heartbeat_since_seal_) && timer) SealCar();
}

// OnCarReady (Alg. 1): the enclave seals the Car and its frontier; the Car
// also carries the PoA of this lane's latest certified Car.
void AutoBahn::SealCar() {
  TeeHost::SealedCar sealed;
  Car car;
  if (!bof_) {
    std::vector<TeeHost::OlItem> items;
    {
      std::lock_guard<std::mutex> lk(ol_mu_);
      size_t take = std::min(ol_open_.size(), static_cast<size_t>(block_size_));
      items.reserve(take);
      for (size_t i = 0; i < take; ++i) {
        items.push_back(std::move(ol_open_.front()));
        ol_open_.pop_front();
      }
    }
    if (!tee_->OlSealCar(items, &sealed)) {
      // Keep the keys for the next seal: dropping them would leave them
      // unsealed in the enclave, so G_r could never advance past them.
      LOG(ERROR) << "OL: enclave refused to seal a Car";
      std::lock_guard<std::mutex> lk(ol_mu_);
      for (auto it = items.rbegin(); it != items.rend(); ++it) {
        ol_open_.push_front(std::move(*it));
      }
      return;
    }
    for (const auto& it : items) {
      OlCarItem* item = car.add_ol_items();
      item->set_txn_hash(it.id);
      item->set_key(it.key);
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
    bof_open_count_ -= static_cast<int>(sealed.bof_items.size());
  }

  car.set_lane(id_);
  car.set_index(sealed.index);
  car.set_prev_hash(last_car_hash_);
  car.set_hash(sealed.hash);
  car.set_tee_tag(sealed.tag);
  car.set_frontier(sealed.frontier);
  car.set_seal_time(Now());
  car.set_admit(static_cast<int32_t>(own_rate_.load()));
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
  NoteHeard(id_);
  lane.hashes[car.index()] = car.hash();
  lane.bodies_held.insert(car.index());
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
  std::string key = "poa" + poa.car_hash();
  PutI64(&key, poa.index());
  if (CheckVerified(key)) return true;
  std::set<int> signers;
  for (const SignInfo& ack : poa.acks()) {
    const int signer = static_cast<int>(ack.sign().node_id());
    if (signer < 1 || signer > n_ || signers.count(signer)) continue;
    if (!verifier_->VerifyMessage(poa.car_hash(), ack.sign())) continue;
    signers.insert(signer);
  }
  if (static_cast<int>(signers.size()) < f_ + 1) return false;
  MarkVerified(key);
  return true;
}

// Stateless checks of a received Car: its hash matches its content, the
// lane's enclave sealed it, and the embedded PoA holds f+1 acknowledgments.
bool AutoBahn::VerifyCar(const Car& car) {
  if (car.lane() < 1 || car.lane() > n_ || car.index() < 1) return false;
  std::string hash;
  if (!bof_) {
    std::vector<TeeHost::OlItem> items;
    items.reserve(car.ol_items_size());
    for (const OlCarItem& it : car.ol_items()) items.push_back({it.txn_hash(), it.key()});
    hash = TeeHost::OlCarHash(car.lane(), car.index(), car.prev_hash(), items,
                              car.frontier());
  } else {
    if (car.bof_items_size() != car.bof_heartbeat_size() ||
        car.m_size() != n_) {
      return false;
    }
    std::vector<TeeHost::BofItem> items;
    items.reserve(car.bof_items_size());
    for (int i = 0; i < car.bof_items_size(); ++i) {
      items.push_back({car.bof_items(i), car.bof_heartbeat(i)});
    }
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
  NoteHeard(car.lane());
  if (car.poa().index() > 0) LearnCertified(car.lane(), car.poa());
  if (car.admit() > 0) {
    reported_rate_[car.lane()] = car.admit();
    reported_at_[car.lane()] = Now();
  }
  const int64_t idx = car.index();
  if (idx <= lane.acked || lane.buffered.count(idx)) return;
  lane.buffered.emplace(idx, car);
  AdvanceLane(lane);
}

// Acknowledges buffered Cars in lane order.  A replica acknowledges a Car
// only once it holds the bodies of all its transactions, so that a PoA's
// f+1 acknowledgments include a correct replica able to serve them.
// Missing bodies (transactions received for the first time in a Car,
// Sec. 5.2) are fetched from the lane's owner, then from every replica.
void AutoBahn::AdvanceLane(Lane& lane) {
  while (!lane.buffered.empty() && lane.buffered.begin()->first == lane.acked + 1) {
    Car& next = lane.buffered.begin()->second;
    std::string prev = next.index() == 1 ? ZeroHash() : lane.hashes[next.index() - 1];
    if (next.prev_hash() != prev) {
      LOG(ERROR) << "lane " << next.lane() << " Car " << next.index()
                 << " does not extend the acknowledged prefix";
      lane.buffered.erase(lane.buffered.begin());
      continue;
    }
    int64_t now = Now();
    if (lane.head_index != next.index()) {
      lane.head_index = next.index();
      lane.head_missing = MissingBodies(CarTxnIds(next));
      if (!lane.head_missing.empty()) {
        RequestBodies(lane.head_missing, next.lane());
        lane.head_requested = now;
      }
    } else if (!lane.head_missing.empty()) {
      lane.head_missing = MissingBodies(lane.head_missing);
      if (!lane.head_missing.empty() &&
          now - lane.head_requested >= 2 * params_.delta_us) {
        RequestBodies(lane.head_missing, 0);
        lane.head_requested = now;
      }
    }
    if (!lane.head_missing.empty()) return;
    if (lane.head_requested > 0) {
      static int64_t last_report = 0;
      if (now - lane.head_requested > params_.delta_us / 2 && now - last_report > 1000000) {
        last_report = now;
        LOG(ERROR) << "lane " << next.lane() << " Car " << next.index()
                   << " waited " << (now - lane.head_requested) << "us for bodies";
      }
      lane.head_requested = 0;
    }
    Car car = std::move(next);
    lane.buffered.erase(lane.buffered.begin());
    AcceptCar(lane, std::move(car));
  }
}

void AutoBahn::AdvanceLanes() {
  for (int j = 1; j <= n_; ++j) {
    Lane& lane = lanes_[j];
    if (j != id_ && !lane.head_missing.empty()) AdvanceLane(lane);
  }
}

void AutoBahn::AcceptCar(Lane& lane, Car car) {
  // Signed and sent by a sender thread.
  auto ack = std::make_unique<CarAck>();
  ack->set_lane(car.lane());
  ack->set_index(car.index());
  ack->set_car_hash(car.hash());
  const int owner = car.lane();
  const int64_t index = car.index();
  const std::string hash = car.hash();
  Outgoing out;
  out.type = MessageType::Car_Ack;
  out.target = owner;
  out.queued = Now();
  out.prepare = [this, index, hash](google::protobuf::Message* msg) {
    SignInfo info;
    info.set_hash(hash);
    info.set_sender_id(id_);
    info.set_local_id(index);
    auto sig = verifier_->SignMessage(hash);
    if (sig.ok()) *info.mutable_sign() = *sig;
    *static_cast<CarAck*>(msg)->mutable_sign() = info;
  };
  out.msg = std::move(ack);
  {
    std::lock_guard<std::mutex> lk(send_mu_);
    send_queue_.push_back(std::move(out));
  }
  send_cv_.notify_one();
  lane.bodies_held.insert(car.index());
  lane.acked = car.index();
  lane.hashes[car.index()] = car.hash();
  lane.cars.emplace(car.index(), std::move(car));
}

void AutoBahn::NoteHeard(int lane) {
  if (all_heard_at_ > 0 || heard_[lane]) return;
  heard_[lane] = true;
  for (int j = 1; j <= n_; ++j) {
    if (!heard_[j]) return;
  }
  all_heard_at_ = Now();
  LOG(ERROR) << "Pearl replica " << id_ << ": every lane has delivered a Car";
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
  // Each ack signs the Car hash; the PoA keeps only the signatures (the
  // hash, index and signer are implied by the PoA and the signature).
  for (const auto& a : acks) *poa.add_acks()->mutable_sign() = a.second.sign();
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
    if (car == nullptr && rt != lane.recent.end()) car = rt->second.get();
    if (car != nullptr) SendMessage(MessageType::Car_Prop, *car, q->requester());
  });
}

const Car* AutoBahn::FindCar(int lane_id, int64_t index) const {
  const Lane& lane = lanes_[lane_id];
  auto it = lane.cars.find(index);
  if (it != lane.cars.end()) return &it->second;
  auto rt = lane.recent.find(index);
  if (rt != lane.recent.end()) return rt->second.get();
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
  auto reject = [&](const char* why) {
    static std::atomic<int64_t> last_report{0};
    if (Now() - last_report > 1000000) {
      last_report = Now();
      LOG(ERROR) << "rejecting block " << b.height() << " of view " << b.view()
                 << " from " << b.proposer() << ": " << why;
    }
    return false;
  };
  if (b.proposer() < 1 || b.proposer() > n_ || b.height() < 1) return reject("header");
  // The digest binds the content, so a copy of a block already verified
  // needs no further signature checks.
  if (BlockDigest(b) != b.hash()) return reject("digest");
  const std::string key = "block" + b.hash();
  if (CheckVerified(key)) return true;
  const QuorumCert& qc = b.parent_qc();
  if (qc.height() != b.height() - 1 || qc.block_hash() != b.parent_hash() ||
      !VerifyQc(qc)) {
    return reject("parent certificate");
  }
  std::set<int> lanes;
  for (const Tip& t : b.tips()) {
    if (t.lane() < 1 || t.lane() > n_ || !lanes.insert(t.lane()).second) {
      return reject("tip lanes");
    }
    if (t.poa().index() != t.index() || t.poa().car_hash() != t.car_hash() ||
        !VerifyPoa(t.lane(), t.poa())) {
      return reject("tip PoA");
    }
  }
  for (const Claim& c : b.claims()) {
    if (!bof_ || c.lane() < 1 || c.lane() > n_) return reject("claim");
  }
  if (!tee_->VerifyBatch({{TeeHost::BlockMsg(b.proposer(), b.view(), b.height(),
                                             b.hash()),
                           b.tee_tag()}})[0]) {
    return reject("TEE tag");
  }
  MarkVerified(key);
  return true;
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
  {
    int64_t delay = Now() - proposal->block().propose_time();
    static std::atomic<int64_t> last_report{0};
    if (delay > params_.delta_us / 2 && Now() - last_report > 1000000) {
      last_report = Now();
      LOG(ERROR) << "proposal " << proposal->block().height() << " from "
                 << proposal->forwarder() << " reached the network thread "
                 << delay << "us after it was proposed";
    }
  }
  if (!VerifyBlock(proposal->block())) return;
  bool with_parent = proposal->has_parent() &&
                     proposal->parent().hash() == proposal->block().parent_hash() &&
                     VerifyBlock(proposal->parent());
  auto p = std::make_shared<Proposal>(std::move(*proposal));
  int64_t posted = Now();
  Post([this, p, with_parent, posted] {
    int64_t wait = Now() - posted;
    static int64_t last_report = 0;
    if (wait > params_.delta_us / 2 && Now() - last_report > 1000000) {
      last_report = Now();
      LOG(ERROR) << "protocol queue: proposal waited " << wait << "us";
    }
    if (with_parent) StoreKnownBlock(p->parent());
    HandleProposal(p->block());
  });
}

// A block this replica missed (lost or dropped proposal): fetched by hash
// from every replica, at most once per 2 Delta.
void AutoBahn::RequestBlock(const std::string& hash) {
  int64_t now = Now();
  int64_t& last = block_requests_[hash];
  if (now - last < 2 * params_.delta_us) return;
  last = now;
  BlockQuery q;
  q.set_requester(id_);
  q.set_hash(hash);
  Broadcast(MessageType::Block_Query, q);
}

void AutoBahn::ReceiveBlockQuery(std::unique_ptr<BlockQuery> query) {
  auto q = std::make_shared<BlockQuery>(std::move(*query));
  Post([this, q] {
    if (q->requester() < 1 || q->requester() > n_ || q->requester() == id_) return;
    auto it = blocks_.find(q->hash());
    if (it == blocks_.end()) return;
    BlockReply reply;
    *reply.mutable_block() = it->second.block;
    SendMessage(MessageType::Block_Reply, reply, q->requester());
  });
}

void AutoBahn::ReceiveBlockReply(std::unique_ptr<BlockReply> reply) {
  if (!VerifyBlock(reply->block())) return;
  auto r = std::make_shared<BlockReply>(std::move(*reply));
  Post([this, r] {
    block_requests_.erase(r->block().hash());
    StoreKnownBlock(r->block());
  });
}

// A certified block learned outside the voting path (parent of a view's
// first proposal, a Status, or a fetch): known, never voted on.
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
// below its last committed position, and every tip is fresh: at least the
// highest certified tip of its lane the voter held Delta before the proposal
// was made (Sec. 5.3: "a correct leader proposing at time t has received
// every certified tip that a correct replica held at t - Delta").  The
// proposal time is the leader's claim, so the voter uses the bound it
// implies under synchrony: receipt <= t + Delta, i.e. tips held since
// receipt - 2 Delta.
bool AutoBahn::ValidTipCut(const BlockState& st) {
  const ConsensusBlock& b = st.block;
  for (int j = 1; j <= n_; ++j) {
    const Tip* t = TipOf(b, j);
    int64_t tip = t == nullptr ? 0 : t->index();
    if (tip < lanes_[j].committed || tip < TipIndex(b.parent_hash(), j)) {
      LOG(ERROR) << "tip cut of block " << b.height() << ": lane " << j
                 << " regresses (tip " << tip << ", committed "
                 << lanes_[j].committed << ", parent tip "
                 << TipIndex(b.parent_hash(), j) << ")";
      return false;
    }
    int64_t held = 0, held_since = 0;
    for (const auto& c : lanes_[j].certified_at) {
      if (c.second <= st.receive_time - 2 * params_.delta_us) {
        held = c.first;
        held_since = c.second;
      }
    }
    if (tip < held) {
      LOG(ERROR) << "tip cut of block " << b.height() << " (proposed "
                 << (st.receive_time - b.propose_time()) << "us before receipt)"
                 << ": lane " << j << " tip " << tip << " is staler than "
                 << held << ", held for " << (st.receive_time - held_since) << "us";
      return false;
    }
  }
  return true;
}

// Checks (ii) and (iii); both delay the vote rather than reject the block.
bool AutoBahn::VoteWaitsOver(const BlockState& st) {
  const ConsensusBlock& b = st.block;
  if (!bof_) {
    // (ii) OL: the collection window closed for every transaction newly
    // referenced by the tip cut.
    for (const Tip& t : b.tips()) {
      for (int64_t idx = TipIndex(b.parent_hash(), t.lane()) + 1; idx <= t.index();
           ++idx) {
        if (idx <= lanes_[t.lane()].committed) continue;
        const Car* car = FindCar(t.lane(), idx);
        if (car == nullptr) {
          RequestCar(t.lane(), idx);
          return false;
        }
        std::vector<TxnId> ids = CarTxnIds(*car);
        if (WindowsClosed(ids)) continue;
        // A transaction never received (the Car was fetched): get it.
        int64_t& asked = body_requests_[{t.lane(), idx}];
        if (Now() - asked >= 2 * params_.delta_us) {
          std::vector<TxnId> missing = MissingBodies(ids);
          if (!missing.empty()) RequestBodies(missing, 0);
          asked = Now();
        }
        return false;
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

// Votes are cast in height order.  A replica votes for a block only if its
// parent certificate ranks at least as high as the replica's lock and its
// height exceeds the last height it voted for in this view; a block that
// fails either is stale (superseded by a certificate the replica already
// holds, or below a block it voted for) and is dropped, never voted for.
// Equivocation within a view is detected through forwarding.
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
    if (RankAbove(highest_qc_, b.parent_qc()) || b.height() <= voted_height_) {
      it = pending_votes_.erase(it);
      continue;
    }
    if (!b.parent_hash().empty() && !blocks_.count(b.parent_hash())) {
      RequestBlock(b.parent_hash());
      break;
    }
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
  voted_height_ = b.height();
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
  std::string h = hash;
  while (h != committed_hash_) {
    if (h.empty()) break;
    auto it = blocks_.find(h);
    if (it == blocks_.end()) {
      // A missed ancestor: fetch it; the chain commits once it is known
      // (a later commit timer walks it again).
      RequestBlock(h);
      return;
    }
    // Under synchrony Sync HotStuff never commits two blocks at one height;
    // reaching a committed block other than the committed tip means the
    // Delta bound was violated.  Stop rather than diverge.
    LOG_IF(FATAL, it->second.committed)
        << "safety violation: block " << it->second.block.height()
        << " conflicts with the committed chain (tip height " << committed_height_
        << "); the synchrony bound Delta was violated";
    if (it->second.queued) break;
    chain.push_back(h);
    h = it->second.block.parent_hash();
  }
  LOG_IF(FATAL, h.empty() && !committed_hash_.empty() && !chain.empty())
      << "safety violation: a block to commit does not extend the committed "
         "chain; the synchrony bound Delta was violated";
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
    // Execution needs every committed transaction's body.
    for (const Tip& t : b.tips()) {
      const Lane& lane = lanes_[t.lane()];
      for (int64_t idx = lane.committed + 1; idx <= t.index(); ++idx) {
        if (lane.bodies_held.count(idx)) continue;  // checked when acknowledged
        std::vector<TxnId> missing = MissingBodies(CarTxnIds(*FindCar(t.lane(), idx)));
        if (missing.empty()) continue;
        complete = false;
        int64_t& asked = body_requests_[{t.lane(), idx}];
        if (Now() - asked >= 2 * params_.delta_us) {
          RequestBodies(missing, 0);
          asked = Now();
        }
      }
    }
    if (!complete) return;

    CommittedBlock out;
    out.height = b.height();
    out.commit_time = Now();
    for (int j = 1; j <= n_; ++j) {
      const Tip* t = TipOf(b, j);
      if (t == nullptr) continue;
      Lane& lane = lanes_[j];
      for (int64_t idx = lane.committed + 1; idx <= t->index(); ++idx) {
        auto it = lane.cars.find(idx);
        std::shared_ptr<const Car> car;
        if (it != lane.cars.end()) {
          car = std::make_shared<const Car>(std::move(it->second));
          lane.cars.erase(it);
        } else {
          car = lane.recent[idx];
        }
        out.cars.push_back(car);
        lane.recent.emplace(idx, std::move(car));
        while (lane.recent.size() > kRecentCars) lane.recent.erase(lane.recent.begin());
      }
      lane.bodies_held.erase(lane.bodies_held.begin(),
                             lane.bodies_held.upper_bound(t->index()));
      if (t->index() > lane.committed) {
        body_requests_.erase(body_requests_.lower_bound({j, 0}),
                             body_requests_.upper_bound({j, t->index()}));
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
    LOG_IF(FATAL, b.parent_hash() != committed_hash_)
        << "safety violation: block " << b.height()
        << " does not extend the committed chain (tip height " << committed_height_
        << "); the synchrony bound Delta was violated";
    st.committed = true;
    committed_height_ = b.height();
    committed_hash_ = b.hash();
    commit_queue_.pop_front();
    if (bof_) {
      // Settlement runs on its own thread, right behind commit.
      out.claims = std::move(claims);
      {
        std::lock_guard<std::mutex> lk(settle_mutex_);
        settle_queue_.push_back(std::move(out));
      }
      settle_cv_.notify_one();
      continue;
    }
    {
      std::lock_guard<std::mutex> lk(exec_mutex_);
      exec_queue_.push_back(std::move(out));
    }
    exec_cv_.notify_one();
  }
}

// BOF settlement (Sec. 7.1) of each committed block, in commit order, on a
// dedicated thread: a large block would otherwise stall the protocol thread.
// The delinquency timers it arms and the claim targets are handed back to the
// protocol thread at once; the block then goes to execution.
void AutoBahn::SettleLoop() {
  while (!IsStop()) {
    CommittedBlock block;
    {
      std::unique_lock<std::mutex> lk(settle_mutex_);
      settle_cv_.wait_for(lk, std::chrono::milliseconds(10),
                          [&] { return !settle_queue_.empty() || IsStop(); });
      if (settle_queue_.empty()) continue;
      block = std::move(settle_queue_.front());
      settle_queue_.pop_front();
    }
    SettleBof(block.claims, &block);
    {
      std::lock_guard<std::mutex> lk(exec_mutex_);
      exec_queue_.push_back(std::move(block));
    }
    exec_cv_.notify_one();
  }
}

// The leader proposes the next block once its previous one is certified,
// as soon as the tip cut advanced or a claim is due, and at least every Delta.
void AutoBahn::MaybePropose() {
  if (!in_view_ || LeaderOf(view_) != id_) return;
  if (all_heard_at_ == 0 && committed_height_ == 0 && last_proposed_.empty()) return;
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
  // At most one block per Delta/5: under load the tip cut advances
  // continuously, and every block (with one PoA per lane) is forwarded by
  // every replica.
  if (now - last_propose_time_ < params_.delta_us / 5) return;

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
  if (!consensus_started_ && all_heard_at_ > 0 &&
      now - all_heard_at_ >= kStartupGraceDeltas * params_.delta_us) {
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
  voted_height_ = 0;
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
// Settlement is a function of the committed prefix only.
void AutoBahn::SettleBof(const std::vector<int>& claims, CommittedBlock* out) {
  std::vector<BofCommittedCar> cars;
  for (const auto& car_ptr : out->cars) {
    const Car& car = *car_ptr;
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
  auto armed = std::make_shared<std::vector<std::pair<int, int64_t>>>(std::move(res.armed));
  auto targets = std::make_shared<std::map<int, int64_t>>(bof_orderer_->ClaimTargets());
  const int64_t committed_at = out->commit_time;
  Post([this, armed, targets, committed_at] {
    ArmDelinquencyTimers(*armed, committed_at);
    claim_targets_ = *targets;
  });
  int64_t lag = Now() - committed_at;
  static int64_t last_report = 0;
  if (lag > params_.delta_us / 2 && Now() - last_report > 1000000) {
    last_report = Now();
    LOG(ERROR) << "bof: settled block " << out->height << " " << lag << "us after commit";
  }
  out->bof_eligible = std::move(res.eligible);
  out->bof_pending = bof_orderer_->PendingSize();
  out->bof_cars = std::move(cars);
}

// The timer starts when the block commits (the paper's rule, so the timers
// of correct replicas expire within Delta of one another), however late
// settlement learns of it.
void AutoBahn::ArmDelinquencyTimers(
    const std::vector<std::pair<int, int64_t>>& armed, int64_t committed_at) {
  for (const auto& a : armed) delinquency_armed_.emplace(a, committed_at);
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

std::vector<AutoBahn::Body> AutoBahn::GetBodies(const std::vector<TxnId>& ids) {
  std::vector<Body> out(ids.size());
  std::vector<std::vector<size_t>> parts(shards_.size());
  for (size_t i = 0; i < ids.size(); ++i) {
    uint64_t v;
    std::memcpy(&v, ids[i].b.data() + 8, sizeof(v));
    parts[v % shards_.size()].push_back(i);
  }
  for (size_t k = 0; k < parts.size(); ++k) {
    if (parts[k].empty()) continue;
    Shard& s = *shards_[k];
    std::lock_guard<std::mutex> lk(s.mu);
    for (size_t i : parts[k]) {
      auto it = s.bodies.find(ids[i]);
      if (it != s.bodies.end()) out[i] = it->second;
    }
  }
  return out;
}

void AutoBahn::Execute(CommittedBlock& block) {
  int64_t now = Now();

  // Newly committed transactions, and consensus latency over those this
  // replica received directly from a client.
  std::vector<TxnId> fresh;
  for (const auto& car_ptr : block.cars) {
    const Car& car = *car_ptr;
    for (const TxnId& id : CarTxnIds(car)) {
      if (exec_committed_.insert(id).second) fresh.push_back(id);
    }
  }
  int samples = 0;
  int64_t latency_sum = 0;
  for (const Body& body : GetBodies(fresh)) {
    if (!body.from_client) continue;
    latency_sum += now - body.first_seen;
    ++samples;
  }
  LOG(ERROR) << "slot_committed slot:" << block.height << " txns:" << fresh.size()
             << " consensus_latency_us:" << (samples ? latency_sum / samples : 0);

  std::vector<std::string> order;
  size_t pending = 0;
  if (!bof_) {
    std::vector<OlCommittedCar> cars;
    for (const auto& car_ptr : block.cars) {
    const Car& car = *car_ptr;
      OlCommittedCar c;
      c.lane = car.lane();
      c.index = car.index();
      c.frontier = car.frontier();
      for (const OlCarItem& it : car.ol_items()) c.items.emplace_back(it.txn_hash(), it.key());
      cars.push_back(std::move(c));
    }
    order = ol_->Commit(cars);
    pending = ol_->PendingSize();
  } else {
    // Hold-back rule (Sec. 7.1), then Alg. 3 on what it releases.
    bof_hold_->OnCommitted(block.bof_cars, block.claims);
    std::vector<BofEligibleTxn> released = bof_hold_->Release(std::move(block.bof_eligible));
    const size_t releasing = released.size();
    int64_t start = Now();
    auto batches = BofOrderer::Order(std::move(released), n_, bof_theta_);
    int64_t took = Now() - start;
    static int64_t last_report = 0;
    if (took > params_.delta_us && Now() - last_report > 1000000) {
      last_report = Now();
      LOG(ERROR) << "bof: ordering " << releasing << " transactions took " << took
                 << "us (" << batches.size() << " batches, " << bof_hold_->HeldSize()
                 << " held back)";
    }
    for (auto& batch : batches) {
      for (auto& id : batch) order.push_back(std::move(id));
    }
    pending = block.bof_pending;
  }

  std::vector<TxnId> ids;
  ids.reserve(order.size());
  for (const std::string& id : order) ids.push_back(TxnId::From(id));
  std::vector<Body> bodies = GetBodies(ids);
  int64_t exec_now = Now();
  int64_t lat_sum = 0, lat_max = 0;
  int lat_samples = 0;
  for (size_t i = 0; i < order.size(); ++i) {
    const Body& body = bodies[i];
    CHECK(body.data != nullptr) << "executing a transaction without its body";
    Transaction txn;
    txn.set_data(*body.data);
    txn.set_hash(order[i]);
    txn.set_id(execute_id_++);
    Commit(txn);
    if (body.from_client) {
      int64_t lat = exec_now - body.first_seen;
      lat_sum += lat;
      lat_max = std::max(lat_max, lat);
      ++lat_samples;
    }
  }
  if (!ids.empty()) {
    // The host drops its copies of executed bodies after a retention period.
    // The enclave keeps H: deleting an id safely would require the enclave
    // to verify an execution proof (not implemented; see the paper).
    std::vector<std::vector<TxnId>> parts(shards_.size());
    for (const TxnId& id : ids) {
      uint64_t v;
      std::memcpy(&v, id.b.data() + 8, sizeof(v));
      parts[v % shards_.size()].push_back(id);
    }
    for (size_t k = 0; k < parts.size(); ++k) {
      if (parts[k].empty()) continue;
      std::lock_guard<std::mutex> lk(shards_[k]->in_mu);
      auto& gc = shards_[k]->gc_inbox;
      gc.insert(gc.end(), parts[k].begin(), parts[k].end());
    }
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
