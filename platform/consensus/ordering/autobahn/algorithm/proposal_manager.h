#pragma once

#include <algorithm>
#include <condition_variable>
#include <list>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "platform/consensus/ordering/autobahn/algorithm/proposal_graph.h"
#include "platform/consensus/ordering/autobahn/proto/proposal.pb.h"
#include "platform/statistic/stats.h"
#include "common/crypto/signature_verifier.h"

namespace resdb { namespace autobahn { class TeeHost; } }  // forward declaration

namespace resdb {
namespace autobahn {

class ProposalManager {
 public:
  ProposalManager(int32_t id, int total_num, int f, SignatureVerifier* verifier);

  // --- Block management (unchanged) ---
  void MakeBlock(std::vector<std::unique_ptr<Transaction>>& txn);
  void AddBlock(std::unique_ptr<Block> block);
  void AddLocalBlock(std::unique_ptr<Block> block);
  const Block* GetLocalBlock(int64_t block_id);
  Block* GetBlock(int sender, int64_t block_id);
  int64_t GetCurrentBlockId();

  void BlockReady(const std::map<int, SignInfo>& sign_info, int64_t local_id);

  SignInfo SignBlock(const Block& block);
  bool VerifyBlock(const Block& block);

  // --- View/slot management (unchanged) ---
  bool ReadyView(int slot);
  int GetCurrentView();
  void IncreaseView();
  void UpdateView(int sender, int64_t block_id);

  std::pair<int, std::map<int, int64_t>> GetCut();
  std::unique_ptr<Proposal> GenerateProposal(int slot, const std::map<int, int64_t>& blocks);

  std::unique_ptr<Proposal> GetProposalData(int slot);
  void AddProposalData(std::unique_ptr<Proposal> p);

  // ===========================================================
  // Fair Ordering: Ordering Linearizability (Algorithm 1, Sec V)
  // ===========================================================

  // Algorithm 1, OnReceiveCar (lines 3-14): TEE timestamps transactions.
  // Returns signed timestamps for all newly-attested transactions.
  // Simulates TEE: monotonic clock, seen-set H, one attestation per txn.
  std::vector<SignedTimestamp> TimestampTransactions(const Block& block);

  // Algorithm 1, OnReceiveTimestamp (lines 16-19): Store a verified timestamp.
  void AddTimestamp(const SignedTimestamp& ts);

  // Algorithm 1, AfterCollectionDeadline (lines 21-24): Compute local
  // ordering key K_r(t) = (f+1)-th smallest collected timestamp for t.
  // Called after Δ has elapsed since receiving the transaction.
  // Returns the computed ordering key, or -1 if not enough timestamps.
  int64_t ComputeLocalOrderingKey(const std::string& txn_hash);

  // Compute ordering keys for all transactions that have enough timestamps.
  // Returns a map of txn_hash -> K_r(t).
  std::map<std::string, int64_t> ComputeAllOrderingKeys();

  // Get transactions in the execution window (τ_prev, τ_current].
  // Returns transactions sorted by ordering key (ascending), with
  // deterministic tie-breaking by txn hash.
  std::vector<std::pair<std::string, int64_t>> GetTransactionsInWindow(
      int64_t tau_prev, int64_t tau_current);

  // --- Execution Threshold (Section V-B) ---

  // Update the last-seen vector for a replica from a committed block.
  void UpdateLastSeenFromBlock(int replica_id, int64_t latest_timestamp);

  // Get this replica's last-seen vector L = <L_1, ..., L_n>.
  std::vector<int64_t> GetLastSeenVector() const;

  // Compute the execution threshold τ = (f+1)-th smallest of M_1, ..., M_n
  // where M_i is the max last-seen timestamp reported by replica i.
  int64_t ComputeExecutionThreshold() const;

  // Get/set the previous execution threshold (τ_prev).
  int64_t GetPrevThreshold() const { return prev_threshold_; }
  void SetPrevThreshold(int64_t tau) { prev_threshold_ = tau; }

  // Get the final ordering key K(t) for a transaction.
  // K(t) = min K_r(t) over the first f+1 committed ordering keys for t.
  int64_t GetFinalOrderingKey(const std::string& txn_hash);

  // Record a committed ordering key for a transaction from a committed block.
  void AddCommittedOrderingKey(const std::string& txn_hash, int64_t key);

  // Check if a transaction has been seen by the TEE (is in seen-set H).
  bool HasSeenTransaction(const std::string& txn_hash) const;

  // ===========================================================
  // Batch-Order Fairness (γ-BOF, Definition 2)
  // ===========================================================

  // Record a replica's local receive ordering for a set of transactions.
  // Called when a RelativeOrdering message is received from a replica.
  void AddRelativeOrdering(const RelativeOrdering& ordering);

  // Record this replica's own receive order for transactions in a block.
  // Returns the RelativeOrdering to broadcast to other replicas.
  RelativeOrdering RecordLocalReceiveOrder(const Block& block);

  // Build the batch-order-fair ordering for transactions in the execution window.
  // Uses the dependency graph: edge t_a → t_b exists if f+1 replicas observed
  // t_a before t_b. Transactions in the same SCC form a batch (unordered).
  // Returns batches in topological order.
  std::vector<std::vector<std::string>> GetBatchOrderedTransactions(
      int64_t tau_prev, int64_t tau_current);

  // Set whether batch-order fairness mode is enabled.
  void SetBatchOrderFairness(bool enabled) { batch_order_fairness_ = enabled; }
  bool IsBatchOrderFairness() const { return batch_order_fairness_; }

  // Set the γ parameter for BOF (edge threshold θ = ⌈γ(f+1)⌉).
  // γ ∈ (0.5, 1.0]; default 1.0 (= f+1, strictest, works for n=2f+1).
  void SetBofGamma(float gamma) { bof_gamma_ = gamma; }

  // ===========================================================
  // Δ-wait and timing parameters
  // ===========================================================

  // Set the Δ parameter (collection deadline, microseconds).
  // After Δ since first receipt, ComputeLocalOrderingKey() uses whatever
  // timestamps have been collected (Algorithm 1 AfterCollectionDeadline).
  void SetDeltaUs(int64_t delta_us) { delta_us_ = delta_us; }

  // ===========================================================
  // SGX TEE integration
  // ===========================================================

  // Set the SGX TeeHost instance. When set, TimestampTransactions() calls
  // the enclave via ECALL instead of the software simulation.
  void SetTeeHost(TeeHost* host) { tee_host_ = host; }

  // Store the ECDSA-P256 public key (64 bytes) for a remote replica's TEE.
  // Called when a TimestampBatch with tee_pubkey is received.
  void SetRemoteTeePublicKey(int sender_id, const std::string& pubkey64) {
    std::unique_lock<std::mutex> lk(ts_mutex_);
    remote_tee_pubkeys_[sender_id] = pubkey64;
  }

  // Get the cached TEE public key for a remote sender (empty if not yet received).
  std::string GetRemoteTeePublicKey(int sender_id) {
    std::unique_lock<std::mutex> lk(ts_mutex_);
    auto it = remote_tee_pubkeys_.find(sender_id);
    return (it != remote_tee_pubkeys_.end()) ? it->second : std::string();
  }

 private:
  void UpdateLastSign(Block * block);

 private:
  int32_t id_;
  int64_t local_block_id_ = 1;

  std::map<int64_t, std::unique_ptr<Block>> pending_blocks_[512];
  std::mutex mutex_, slot_mutex_, p_mutex_;
  std::map<int, std::unique_ptr<Block>> blocks_candidates_;

  std::map<int, std::pair<int, int64_t>> slot_state_;
  std::map<int,int> new_blocks_;

  int total_num_;
  int f_;
  int64_t current_height_;
  int current_slot_;

  SignatureVerifier* verifier_;
  TeeHost* tee_host_ = nullptr;  // non-owning; set via SetTeeHost()

  // Per-sender ECDSA-P256 public keys received from remote TEEs (64 bytes each).
  std::map<int, std::string> remote_tee_pubkeys_;

  std::map<int, std::unique_ptr<Proposal> > pending_proposals_;

  // ===========================================================
  // Fair Ordering State
  // ===========================================================

  // Algorithm 1, line 1: ts_store[txn_hash] = list of signed timestamps
  // Collected TEE-signed timestamps from all replicas for each transaction.
  std::map<std::string, std::vector<SignedTimestamp>> ts_store_;
  std::mutex ts_mutex_;

  // Algorithm 1, line 2: H = seen-set (inside TEE).
  // Set of transaction hashes that this replica's TEE has already attested.
  std::set<std::string> tee_seen_set_;

  // Simulated TEE monotonic clock (microsecond granularity).
  int64_t tee_clock_ = 0;

  // Local ordering keys K_r(t) computed by this replica.
  // Keyed by txn_hash.
  std::map<std::string, int64_t> local_ordering_keys_;
  std::mutex ordering_mutex_;

  // Final ordering keys K(t) = min over first f+1 committed keys.
  // committed_keys_[txn_hash] = sorted list of committed K_r values.
  std::map<std::string, std::vector<int64_t>> committed_keys_;

  // Execution threshold state (Section V-B).
  // last_seen_[i] = M_i = max last-seen timestamp from replica i across
  // all committed blocks.
  std::vector<int64_t> last_seen_;

  // Previous execution threshold τ_prev.
  int64_t prev_threshold_ = 0;

  // ===========================================================
  // Batch-Order Fairness State
  // ===========================================================
  bool batch_order_fairness_ = false;
  float bof_gamma_ = 1.0f;  // γ ∈ (0.5, 1.0]; edge threshold θ = ⌈γ(f+1)⌉

  // Per-replica receive orderings: receive_orders_[replica_id] is a list of
  // txn hashes in the order that replica observed them.
  std::map<int, std::vector<std::string>> receive_orders_;
  std::mutex bof_mutex_;

  // Hash function for string pairs used in the BOF maps below.
  struct PairHash {
    size_t operator()(const std::pair<std::string, std::string>& p) const {
      size_t h1 = std::hash<std::string>{}(p.first);
      size_t h2 = std::hash<std::string>{}(p.second);
      // Asymmetric mix so (a,b) and (b,a) hash differently.
      return h1 ^ (h2 * 2654435761ULL);
    }
  };

  // Pairwise precedence counts: precedes_count_[{a,b}] = number of blocks
  // (up to f+1) where replica observed a before b.
  // Uses unordered_map for O(1) amortized lookup (vs O(log n) for std::map),
  // critical for large transaction sets (Algorithm 5 is O(|T|²)).
  std::unordered_map<std::pair<std::string, std::string>, int, PairHash>
      precedes_count_;

  // Per-pair contribution count: how many block-orderings have been counted
  // toward this pair so far. Capped at f+1 (Algorithm 5 "first f+1 blocks").
  std::unordered_map<std::pair<std::string, std::string>, int, PairHash>
      pair_contribution_count_;

  // Deduplication: blocks for which we have already recorded a relative
  // ordering (keyed by "sender_id_block_sender_id_block_local_id").
  std::unordered_set<std::string> bof_seen_blocks_;

  // Set of all transaction hashes known to the BOF system.
  std::set<std::string> bof_known_txns_;

  // Track which transactions have already been committed via BOF.
  std::set<std::string> bof_committed_txns_;

  // ===========================================================
  // Δ-wait state
  // ===========================================================

  // Δ parameter in microseconds (collection deadline for OL).
  int64_t delta_us_ = 200000;  // default: 200 ms

  // Time (microseconds since epoch) when each transaction was first seen.
  // Used to enforce the AfterCollectionDeadline check.
  std::map<std::string, int64_t> txn_first_seen_us_;

  // BOF sequence numbers assigned by TEE (ecall_assign_sequence_number).
  // Stored as the ordering_key in the SignedTimestamp for BOF transactions.
  std::map<std::string, int64_t> bof_seq_;
};

}  // namespace autobahn
}  // namespace resdb
