#pragma once

#include <deque>
#include <map>
#include <queue>
#include <set>
#include <thread>

#include "platform/common/queue/lock_free_queue.h"
#include "platform/consensus/ordering/common/algorithm/protocol_base.h"
#include "platform/consensus/ordering/autobahn/algorithm/proposal_manager.h"
#include "platform/consensus/ordering/autobahn/proto/proposal.pb.h"
#include "platform/statistic/stats.h"

namespace resdb {
namespace autobahn {

// AutoBahn with Sync HotStuff consensus and Fair Ordering (Ordering Linearizability).
//
// Implements the protocol from "Fair Ordering via Trusted DAG":
//
//   Data Dissemination Layer (TEE-Enabled Autobahn, Algorithm 1):
//     - Blocks (Cars) are broadcast and certified via PoA (f+1 ACKs)
//     - Each replica TEE-timestamps every transaction it receives
//     - Timestamps are broadcast and collected from all replicas
//     - After Δ, local ordering key K_r(t) = (f+1)-th smallest timestamp
//
//   Consensus Layer (Pre-Ordered Sync HotStuff, Algorithms 2-3):
//     - Leader extracts transactions in execution window (τ_prev, τ_current]
//     - Transactions sorted by ordering key K(t) before inclusion
//     - Replicas validate ordering and vote
//     - After 2Δ without equivocation, commit in fair order
//
//   Execution Threshold (Section V-B):
//     - τ = (f+1)-th smallest of max last-seen timestamps per replica
//     - Guarantees no future transaction can have K(t') ≤ τ (Lemma 5)
class AutoBahn: public common::ProtocolBase {
 public:
  AutoBahn(int id, int f, int total_num, int block_size, SignatureVerifier* verifier);
  ~AutoBahn();

  bool ReceiveTransaction(std::unique_ptr<Transaction> txn);
  void ReceiveBlock(std::unique_ptr<Block> block);
  void ReceiveBlockACK(std::unique_ptr<BlockACK> block);

  // Sync HotStuff message handlers
  bool ReceiveProposal(std::unique_ptr<Proposal> proposal);
  bool ReceiveVote(std::unique_ptr<Proposal> vote);
  bool ReceiveEquivocation(std::unique_ptr<EquivocationProof> proof);

  // Fair ordering: TEE timestamp collection
  void ReceiveTimestamps(std::unique_ptr<TimestampBatch> batch);

 private:
  bool IsStop();
  void GenerateBlocks();
  void AsyncDissemination();
  void AsyncConsensus();     // Leader: propose with fair ordering (Algorithm 2)
  void AsyncCommitTimer();   // Timer-based commit: wait 2Δ, then commit

  bool WaitForResponse(int64_t block_id);
  void BlockDone();

  void NotifyView();
  bool WaitForNextView(int view);

  bool WaitForNextLeader();
  void StartNextLeader(int slot);

  void Commit(std::unique_ptr<Proposal> proposal);

  // Equivocation detection
  bool DetectEquivocation(const Proposal& proposal);

 private:
  std::condition_variable bc_block_cv_, view_cv_, leader_cv_;
  LockFreeQueue<Transaction> txns_;
  std::unique_ptr<ProposalManager> proposal_manager_;
  SignatureVerifier* verifier_;
  int execute_id_;

  int id_, total_num_, f_, batch_size_;
  std::atomic<int> is_stop_;
  int timeout_ms_;
  int64_t delta_ms_;  // Synchronous network delay bound (Δ)

  std::thread block_thread_, dissemi_thread_, consensus_thread_, commit_thread_;

  std::mutex block_mutex_, bc_mutex_, view_mutex_, vote_mutex_, commit_mutex_, leader_mutex_;
  std::map<int, std::map<int, SignInfo>> block_ack_;

  // Sync HotStuff vote tracking
  std::map<int, std::map<int, std::unique_ptr<Proposal>>> vote_ack_;

  Stats* global_stats_;
  std::map<int, int64_t> commit_block_;

  bool is_leader_;
  int cur_slot_;

  // Equivocation detection state
  std::map<int, std::string> seen_proposal_hash_;
  std::set<int> equivocated_views_;
  std::mutex equivocation_mutex_;

  // Pending proposals waiting for 2Δ timer
  struct PendingCommit {
    std::unique_ptr<Proposal> proposal;
    int64_t receive_time;
  };
  std::map<int, PendingCommit> pending_commits_;
  std::mutex pending_commit_mutex_;
};

}  // namespace autobahn
}  // namespace resdb
