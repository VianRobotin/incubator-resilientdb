#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "platform/consensus/ordering/autobahn/algorithm/bof_ordering.h"
#include "platform/consensus/ordering/autobahn/algorithm/ol_ordering.h"
#include "platform/consensus/ordering/autobahn/proto/proposal.pb.h"
#include "platform/consensus/ordering/autobahn/tee/tee_host.h"
#include "platform/consensus/ordering/common/algorithm/protocol_base.h"
#include "platform/statistic/stats.h"

namespace pearl {
class AesCmac;
}  // namespace pearl

namespace resdb {
namespace autobahn {

// id(t): the SHA-256 digest of a transaction, as a fixed-size key.
struct TxnId {
  std::array<uint8_t, 32> b;
  bool operator==(const TxnId& o) const { return b == o.b; }
  template <typename H>
  friend H AbslHashValue(H h, const TxnId& id) {
    uint64_t v;
    std::memcpy(&v, id.b.data(), sizeof(v));
    return H::combine(std::move(h), v);
  }
  static TxnId From(const std::string& s) {
    TxnId id;
    id.b.fill(0);
    std::memcpy(id.b.data(), s.data(), std::min<size_t>(s.size(), 32));
    return id;
  }
  std::string Str() const {
    return std::string(reinterpret_cast<const char*>(b.data()), 32);
  }
};

// Pearl: TEE-enabled fair ordering with minimal replication.
//
// Implements the paper's three layers (Sec. 5-7):
//
//  Dissemination (Sec. 5.2, Alg. 1): every replica attests each transaction
//  on first reception (TEE timestamp in OL, Car placement in BOF), echoes it
//  with that evidence, and seals Cars into its lane when full or every
//  X = Delta.  Cars carry the TEE-derived frontier (G_r in OL, (w_r, M_r) in
//  BOF) and the PoA of the lane's latest certified Car.  A replica acks Car i
//  of a lane only once it acked Car i-1.
//
//  Consensus (Sec. 5.3, Alg. 2): Sync HotStuff with a stable leader.  A block
//  carries the certificate of its parent, a tip cut (one certified tip per
//  lane) and, in BOF, delinquency claims.  Voters check (i) a valid and
//  fresh tip cut, (ii) in OL, that their collection window closed for every
//  newly referenced transaction, (iii) in BOF, that their delinquency timer
//  expired for every claim; they then forward the proposal, broadcast their
//  vote and commit after 2 Delta without equivocation.  Leaders are replaced
//  through Sync HotStuff's blame / view-change rules, with the (3p+4)Delta
//  blame budget.
//
//  Execution (Sec. 6-7): committed blocks are handed, in order, to the OL or
//  BOF orderer, which derives the execution order from committed Cars alone.
//
// Threads: network handlers verify signatures and TEE tags (stateless).
// Transactions and echoes go to `dissemination_threads` shards, partitioned
// by id(t), which run OnFirstReceive, store the evidence and close the
// collection windows (batched enclave calls).  Everything else is posted to
// the protocol thread, which owns lane and consensus state and runs the
// timers; it is never queued behind transaction traffic.  The execution
// thread runs the orderers.
class AutoBahn : public common::ProtocolBase {
 public:
  static constexpr int kAdmitAll = 10000;

  struct Params {
    bool batch_order_fairness = false;
    double bof_gamma = 1.0;
    int64_t delta_us = 50000;      // Delta
    int64_t heartbeat_us = 50000;  // idle period before a heartbeat
    int blame_p = 1;               // p of the (3p+4)Delta blame budget
    int dissemination_threads = 4;
    int64_t link_bytes_per_sec = 125000000;  // admission control
    std::string enclave_path;
    std::string group_seed;        // 32 bytes
  };

  AutoBahn(int id, int f, int total_num, int block_size,
           SignatureVerifier* verifier, const Params& params);
  ~AutoBahn();

  // Starts the protocol and execution threads; call once the framework has
  // installed the send/broadcast/commit callbacks.
  void Start();

  // Optional bulk channel: echoes travel on separate connections, so that
  // consensus and lane messages never wait behind them; `backlog` reports
  // the bytes queued on all outgoing connections (admission control).
  using BulkSendFunc = std::function<void(int, const google::protobuf::Message&)>;
  void SetBulkChannel(BulkSendFunc send, std::function<int64_t()> backlog) {
    bulk_send_ = std::move(send);
    outbound_backlog_ = std::move(backlog);
  }

  // `data` is the client request; id(t) = SHA-256(data).
  bool ReceiveTransaction(std::string data);
  void ReceiveEchoBatch(std::unique_ptr<EchoBatch> batch);
  void ReceiveCar(std::unique_ptr<Car> car);
  void ReceiveCarAck(std::unique_ptr<CarAck> ack);
  void ReceiveCarQuery(std::unique_ptr<CarQuery> query);
  void ReceiveBodyQuery(std::unique_ptr<BodyQuery> query);
  void ReceiveBodyReply(std::unique_ptr<BodyReply> reply);
  void ReceiveBlockQuery(std::unique_ptr<BlockQuery> query);
  void ReceiveBlockReply(std::unique_ptr<BlockReply> reply);
  void ReceiveProposal(std::unique_ptr<Proposal> proposal);
  void ReceiveVote(std::unique_ptr<Vote> vote);
  void ReceiveBlame(std::unique_ptr<Blame> blame);
  void ReceiveQuitView(std::unique_ptr<QuitView> quit);
  void ReceiveStatus(std::unique_ptr<Status> status);

 private:
  // ---- Dissemination shards ----
  struct Evidence {
    int32_t signer = 0;
    int64_t value = 0;
    std::array<char, TeeHost::kTagLen> tag;
  };
  // A transaction reaching a shard: from a client, an echo (with the
  // sender's evidence) or a body fetch.
  struct Incoming {
    TxnId id;
    std::shared_ptr<const std::string> data;
    int32_t signer = 0;        // > 0: echo carrying this replica's evidence
    int64_t value = 0;
    std::array<char, TeeHost::kTagLen> tag;
    int64_t client_time = 0;   // > 0: received directly from a client
  };
  // Collection-window state of a transaction (Alg. 1, timestamps[t] /
  // placements[t]), until the window closes.
  struct Record {
    int64_t first_seen = 0;
    std::vector<Evidence> evidence;
  };
  struct Body {
    std::shared_ptr<const std::string> data;
    int64_t first_seen = 0;    // first arrival at this replica
    bool from_client = false;  // also received directly from a client
  };
  struct Shard {
    // Input queue (any thread -> shard thread).
    std::mutex in_mu;
    std::condition_variable in_cv;
    std::vector<Incoming> inbox;
    std::vector<TxnId> gc_inbox;
    std::atomic<size_t> inbox_size{0};
    // State; the shard thread holds `mu` while processing, other threads
    // take it for lookups.
    std::mutex mu;
    absl::flat_hash_set<TxnId> seen;
    absl::flat_hash_map<TxnId, Record> open;
    absl::flat_hash_map<TxnId, Body> bodies;
    std::deque<std::pair<int64_t, TxnId>> windows;
    std::deque<std::pair<int64_t, TxnId>> retry;
    // Shard thread only.
    EchoBatch echo_out;
    int64_t echo_since = 0;
    std::deque<std::pair<int64_t, std::vector<TxnId>>> gc_pending;
    std::thread thread;
  };

  struct Lane {
    std::map<int64_t, Car> cars;        // acknowledged, not yet committed
    // Bodies still missing for the next Car to acknowledge (index
    // head_index), and when they were last requested.
    int64_t head_index = 0;
    std::vector<TxnId> head_missing;
    int64_t head_requested = 0;
    std::map<int64_t, std::string> hashes;
    std::map<int64_t, Car> buffered;    // waiting for their predecessor
    // Recently committed (for queries); shared with the execution pipeline.
    std::map<int64_t, std::shared_ptr<const Car>> recent;
    std::set<int64_t> bodies_held;      // acknowledged with all bodies held
    std::map<int64_t, int64_t> queried; // index -> last query time
    int64_t buffer_since = 0;
    int64_t acked = 0;
    int64_t committed = 0;
    // Highest certified Car known, and when each certification was learned
    // (for the freshness check).
    Poa certified;
    std::deque<std::pair<int64_t, int64_t>> certified_at;  // (index, time)
  };

  struct BlockState {
    ConsensusBlock block;
    int64_t receive_time = 0;
    bool known_only = false;     // learned as a certified ancestor
    bool tipcut_checked = false;
    bool rejected = false;       // failed check (i): never vote
    bool voted = false;
    bool queued = false;         // in the commit queue
    bool committed = false;
  };

  struct CommitTimer {
    int64_t expire = 0;
    std::string hash;
    int64_t view = 0;
  };

  // A committed block as handed to the execution thread.  In BOF,
  // settlement already ran at commit (it arms the delinquency timers), so the
  // block carries the transactions it made eligible.
  struct CommittedBlock {
    int64_t height = 0;
    std::vector<std::shared_ptr<const Car>> cars;  // commitment order
    std::vector<BofEligibleTxn> bof_eligible;
    size_t bof_pending = 0;
    std::vector<int> claims;  // BOF: lanes named by newly committed claims
    std::vector<BofCommittedCar> bof_cars;  // BOF: the Cars, as settled
    int64_t commit_time = 0;
  };

  // Outgoing messages are copied and queued; a pool of sender threads
  // serializes, signs and sends them, so that signing never delays the
  // protocol or dissemination threads.  These hide ProtocolBase's versions.
  int Broadcast(int msg_type, const google::protobuf::Message& msg);
  int SendMessage(int msg_type, const google::protobuf::Message& msg, int node_id);
  void SendLoop();

  // Admission control under overload (see ReceiveTransaction).
  void AdjustAdmission();
  void AdmissionLoop();

  // Signature checks run once per distinct PoA and block: proposals are
  // forwarded by every voter and tips recur across blocks.
  bool CheckVerified(const std::string& key);
  void MarkVerified(const std::string& key);

  // Protocol-thread plumbing.
  bool NetworkReady();
  void Post(std::function<void()> fn);
  void ProtocolLoop();
  void Tick();

  // Dissemination shards.
  Shard& ShardOf(const TxnId& id);
  void Route(std::vector<Incoming> items);
  void ShardLoop(Shard* shard);
  void ShardFirstReceive(Shard& shard, std::vector<Incoming>& batch);
  void ShardCloseWindows(Shard& shard);
  void FlushEchoes(Shard& shard, bool force);
  std::vector<TxnId> MissingBodies(const std::vector<TxnId>& ids);
  bool WindowsClosed(const std::vector<TxnId>& ids);
  void RequestBodies(const std::vector<TxnId>& ids, int target);
  std::vector<TxnId> CarTxnIds(const Car& car) const;

  // Dissemination layer.
  void MaybeHeartbeat();
  void MaybeSealCar();
  void SealCar();
  SignInfo SignAck(const Car& car);
  bool VerifyPoa(int lane, const Poa& poa);
  bool VerifyCar(const Car& car);
  void HandleCar(const Car& car);
  void AdvanceLane(Lane& lane);
  void AdvanceLanes();
  void AcceptCar(Lane& lane, Car car);
  void LearnCertified(int lane, const Poa& poa);
  void NoteHeard(int lane);
  void HandleCarAck(const CarAck& ack);
  void RequestCar(int lane, int64_t index);
  void RequestMissingCars();
  const Car* FindCar(int lane, int64_t index) const;

  // Consensus layer.
  int LeaderOf(int64_t view) const;
  static bool RankAbove(const QuorumCert& a, const QuorumCert& b);
  std::string BlockDigest(const ConsensusBlock& b) const;
  std::string VoteDigest(int64_t view, int64_t height,
                         const std::string& hash) const;
  std::string BlameDigest(int64_t view) const;
  bool VerifyVote(const Vote& v);
  bool VerifyQc(const QuorumCert& qc);
  bool VerifyBlock(const ConsensusBlock& b);
  bool VerifyBlame(const Blame& b);
  const Tip* TipOf(const ConsensusBlock& b, int lane) const;
  int64_t TipIndex(const std::string& block_hash, int lane) const;
  void StoreKnownBlock(const ConsensusBlock& b);
  void RequestBlock(const std::string& hash);
  void HandleProposal(const ConsensusBlock& b);
  bool ValidTipCut(const BlockState& st);
  bool VoteWaitsOver(const BlockState& st);
  void ProcessPendingVotes();
  void CastVote(BlockState& st);
  void HandleVote(const Vote& v);
  void CheckCommitTimers();
  void EnqueueCommitChain(const std::string& hash);
  void ProcessPendingCommits();
  void MaybePropose();
  void CheckBlame();
  void HandleBlame(const Blame& blame);
  void QuitCurrentView();
  void EnterNextView();

  // BOF settlement at commit and delinquency timers (Sec. 7.1).
  void SettleBof(const std::vector<int>& claims, CommittedBlock* out);
  void SettleLoop();
  void ArmDelinquencyTimers(const std::vector<std::pair<int, int64_t>>& armed,
                            int64_t committed_at);
  void CheckDelinquencyTimers();

  // Execution layer.
  void ExecutionLoop();
  void Execute(CommittedBlock& block);
  std::vector<Body> GetBodies(const std::vector<TxnId>& ids);

  // ---- Configuration ----
  const int n_;
  const int block_size_;
  const Params params_;
  const bool bof_;
  int bof_theta_ = 0;  // theta = ceil(gamma (n - f)), fixed at construction
  SignatureVerifier* verifier_;
  Stats* global_stats_;
  std::unique_ptr<TeeHost> tee_;

  // ---- Threads and queues ----
  struct Outgoing {
    int type = 0;
    int target = 0;  // 0: broadcast
    int64_t queued = 0;
    std::unique_ptr<google::protobuf::Message> msg;
    // Run on the sender thread before sending (e.g. to sign).
    std::function<void(google::protobuf::Message*)> prepare;
  };
  std::mutex send_mu_;
  std::condition_variable send_cv_;
  std::deque<Outgoing> send_queue_;
  std::vector<std::thread> send_threads_;
  std::mutex verified_mu_;
  absl::flat_hash_set<std::string> verified_;
  std::deque<std::string> verified_order_;
  std::thread protocol_thread_, execution_thread_;
  std::mutex event_mutex_;
  std::condition_variable event_cv_;
  std::deque<std::function<void()>> events_;
  std::mutex exec_mutex_;
  std::condition_variable exec_cv_;
  std::deque<CommittedBlock> exec_queue_;

  // ---- Dissemination state ----
  std::vector<std::unique_ptr<Shard>> shards_;
  std::atomic<int64_t> dropped_{0};       // client txns shed under overload
  std::atomic<int> admit_{kAdmitAll};     // admission threshold, of kAdmitAll
  std::unique_ptr<pearl::AesCmac> admission_mac_;  // keyed rank of client txns
  // Admission thread: own_rate_ is the admitted rate this replica proposes
  // (read by SealCar); reported_rate_/reported_at_ hold the rates other
  // replicas reported in their Cars (written by the protocol thread).
  std::atomic<double> own_rate_{0};
  std::unique_ptr<std::atomic<int64_t>[]> reported_rate_, reported_at_;
  std::thread admission_thread_;
  double capacity_ = 0;                   // estimated, tx/s
  double arrival_rate_ = 0;               // client arrivals, tx/s
  int64_t window_arrivals_ = 0;
  double link_used_ = 0;
  int64_t nic_rx_ = -1, nic_tx_ = -1;    // traffic counters at last check
  std::atomic<int64_t> client_arrivals_{0};  // since the last adjustment
  std::atomic<int64_t> echo_bytes_{0};       // sent, fan-out included
  std::atomic<int64_t> echoed_txns_{0};
  int admission_ticks_ = 0;
  BulkSendFunc bulk_send_;
  std::function<int64_t()> outbound_backlog_;
  std::mutex ol_mu_;
  std::deque<TeeHost::OlItem> ol_open_;   // OL: keys awaiting a seal
  std::atomic<int> bof_open_count_{0};    // BOF: items in the open Car
  std::atomic<int64_t> last_activity_{0};
  bool heartbeat_since_seal_ = false;     // OL (protocol thread)
  std::atomic<bool> network_ready_{false};
  int64_t start_time_ = 0;
  int64_t last_seal_ = 0;
  int64_t last_heartbeat_ = 0;
  std::string last_car_hash_;
  std::map<int64_t, std::map<int, SignInfo>> own_acks_;
  Poa own_poa_;
  std::vector<Lane> lanes_;

  // ---- Consensus state (protocol thread) ----
  int64_t view_ = 1;
  bool in_view_ = true;
  int64_t view_start_ = 0;
  int64_t quit_time_ = 0;
  bool blamed_ = false;
  bool consensus_started_ = false;
  std::vector<bool> heard_;    // startup: lanes that delivered a Car
  int64_t all_heard_at_ = 0;
  std::map<std::string, BlockState> blocks_;
  std::map<std::pair<int64_t, int64_t>, std::string> seen_heights_;
  std::set<std::pair<int64_t, std::string>> pending_votes_;  // (height, hash)
  std::map<std::string, std::map<int, Vote>> votes_;
  std::map<std::string, QuorumCert> qcs_;
  QuorumCert highest_qc_;  // lock
  QuorumCert base_qc_;     // certificate the leader's first block extends
  std::map<int64_t, std::map<int, Blame>> blames_;
  std::map<int64_t, std::vector<ConsensusBlock>> future_blocks_;
  std::map<int64_t, std::map<int, Status>> statuses_;
  std::deque<int64_t> vote_times_;
  std::vector<CommitTimer> commit_timers_;
  int64_t voted_height_ = 0;  // last height voted for in this view
  std::deque<std::string> commit_queue_;
  int64_t committed_height_ = 0;
  std::string committed_hash_;
  std::string last_proposed_;
  int64_t last_propose_time_ = 0;
  bool leader_ready_ = false;
  // BOF delinquency timers: (lane, position) -> arm time.
  std::map<std::pair<int, int64_t>, int64_t> delinquency_armed_;
  std::set<std::pair<int, int64_t>> delinquency_expired_;
  std::map<int, int64_t> claim_targets_;
  // Block fetches for missed blocks: hash -> last request time.
  std::map<std::string, int64_t> block_requests_;
  // Body fetches for Cars learned outside the lane (lane, index) -> time.
  std::map<std::pair<int, int64_t>, int64_t> body_requests_;
  std::set<int> claimed_lanes_;    // claimed in this view, not yet committed
  std::set<int> discarded_lanes_;  // named by a committed claim

  // BOF settlement state (settlement thread).
  std::unique_ptr<BofOrderer> bof_orderer_;
  std::thread settle_thread_;
  std::mutex settle_mutex_;
  std::condition_variable settle_cv_;
  std::deque<CommittedBlock> settle_queue_;

  // ---- Execution state (execution thread) ----
  std::unique_ptr<OlOrderer> ol_;
  std::unique_ptr<BofHoldBack> bof_hold_;
  absl::flat_hash_set<TxnId> exec_committed_;
  int64_t execute_id_ = 1;
  std::string order_digest_;
};

}  // namespace autobahn
}  // namespace resdb
