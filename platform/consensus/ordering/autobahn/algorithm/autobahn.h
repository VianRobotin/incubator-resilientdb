#pragma once

#include <condition_variable>
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

#include "platform/consensus/ordering/autobahn/algorithm/bof_ordering.h"
#include "platform/consensus/ordering/autobahn/algorithm/ol_ordering.h"
#include "platform/consensus/ordering/autobahn/proto/proposal.pb.h"
#include "platform/consensus/ordering/autobahn/tee/tee_host.h"
#include "platform/consensus/ordering/common/algorithm/protocol_base.h"
#include "platform/statistic/stats.h"

namespace resdb {
namespace autobahn {

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
// Threads: network handlers verify signatures and TEE tags (stateless) and
// post the message to the protocol thread, which owns all lane and consensus
// state and runs the timers; the execution thread runs the orderers.
class AutoBahn : public common::ProtocolBase {
 public:
  struct Params {
    bool batch_order_fairness = false;
    double bof_gamma = 1.0;
    int64_t delta_us = 50000;      // Delta
    int64_t heartbeat_us = 50000;  // idle period before a heartbeat
    int blame_p = 1;               // p of the (3p+4)Delta blame budget
    std::string enclave_path;
    std::string group_seed;        // 32 bytes
  };

  AutoBahn(int id, int f, int total_num, int block_size,
           SignatureVerifier* verifier, const Params& params);
  ~AutoBahn();

  // Starts the protocol and execution threads; call once the framework has
  // installed the send/broadcast/commit callbacks.
  void Start();

  bool ReceiveTransaction(std::unique_ptr<Transaction> txn);
  void ReceiveEchoBatch(std::unique_ptr<EchoBatch> batch);
  void ReceiveCar(std::unique_ptr<Car> car);
  void ReceiveCarAck(std::unique_ptr<CarAck> ack);
  void ReceiveCarQuery(std::unique_ptr<CarQuery> query);
  void ReceiveProposal(std::unique_ptr<Proposal> proposal);
  void ReceiveVote(std::unique_ptr<Vote> vote);
  void ReceiveBlame(std::unique_ptr<Blame> blame);
  void ReceiveQuitView(std::unique_ptr<QuitView> quit);
  void ReceiveStatus(std::unique_ptr<Status> status);

 private:
  // Dissemination state of a transaction until its collection window closed.
  struct TxnRecord {
    int64_t first_seen = 0;
    std::map<int, TeeHost::SignedIndicator> evidence;  // by signer
  };

  struct Lane {
    std::map<int64_t, Car> cars;        // acknowledged, not yet committed
    std::map<int64_t, std::string> hashes;
    std::map<int64_t, Car> buffered;    // waiting for their predecessor
    std::map<int64_t, Car> recent;      // recently committed (for queries)
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

  // A committed block as handed to the execution thread.
  struct CommittedBlock {
    int64_t height = 0;
    std::vector<Car> cars;     // commitment order
    std::vector<int> claims;   // lanes named by newly committed claims
  };

  // Protocol-thread plumbing.
  bool NetworkReady();
  void Post(std::function<void()> fn);
  void ProtocolLoop();
  void Tick();

  // Dissemination layer.
  void OnFirstReceive(const Transaction& txn);
  void QueueEcho(const Transaction& txn, int64_t value, const std::string& tag);
  void FlushEchoes();
  void HandleEchoBatch(const EchoBatch& batch);
  void CloseCollectionWindows();
  void MaybeHeartbeat();
  void MaybeSealCar();
  void SealCar();
  SignInfo SignAck(const Car& car);
  bool VerifyPoa(int lane, const Poa& poa);
  bool VerifyCar(const Car& car);
  void HandleCar(const Car& car);
  void AcceptCar(Lane& lane, Car car);
  void LearnCertified(int lane, const Poa& poa);
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

  // BOF delinquency timers (Sec. 7.1).
  void ArmDelinquencyTimers(const std::vector<std::pair<int, int64_t>>& armed);
  void CheckDelinquencyTimers();

  // Execution layer.
  void ExecutionLoop();
  void Execute(const CommittedBlock& block);

  // ---- Configuration ----
  const int n_;
  const int block_size_;
  const Params params_;
  const bool bof_;
  SignatureVerifier* verifier_;
  Stats* global_stats_;
  std::unique_ptr<TeeHost> tee_;

  // ---- Threads and queues ----
  std::thread protocol_thread_, execution_thread_;
  std::mutex event_mutex_;
  std::condition_variable event_cv_;
  std::deque<std::function<void()>> events_;
  std::mutex exec_mutex_;
  std::condition_variable exec_cv_;
  std::deque<CommittedBlock> exec_queue_;

  // ---- Dissemination state (protocol thread) ----
  std::unordered_set<std::string> seen_;
  std::unordered_map<std::string, TxnRecord> txns_;
  std::unordered_map<std::string, Transaction> bodies_;  // until sealed
  std::deque<std::pair<int64_t, std::string>> windows_;  // (due, id)
  std::deque<std::pair<int64_t, std::string>> retry_windows_;
  std::vector<Echo> echo_out_;
  int64_t last_echo_flush_ = 0;
  std::vector<TeeHost::OlItem> ol_open_;  // OL: keys awaiting a seal
  int open_count_ = 0;                    // BOF: items in the open Car
  bool heartbeat_since_seal_ = false;     // OL
  bool network_ready_ = false;
  int64_t start_time_ = 0;
  int64_t last_seal_ = 0;
  int64_t last_activity_ = 0;
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
  std::string voted_tip_;  // last block voted in this view
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
  std::set<int> claimed_lanes_;    // claimed in this view, not yet committed
  std::set<int> discarded_lanes_;  // named by a committed claim

  // ---- Execution state (execution thread) ----
  std::unique_ptr<OlOrderer> ol_;
  std::unique_ptr<BofOrderer> bof_orderer_;
  std::unordered_map<std::string, Transaction> exec_bodies_;
  std::unordered_set<std::string> exec_committed_;
  int64_t execute_id_ = 1;
  std::string order_digest_;

  // Receipt times of transactions received directly from a client (latency).
  std::mutex create_mutex_;
  std::unordered_map<std::string, int64_t> create_times_;
};

}  // namespace autobahn
}  // namespace resdb
