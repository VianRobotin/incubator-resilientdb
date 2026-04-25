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

  // BOF analogue of GetFinalOrderingKey: the TEE-assigned sequence number
  // for a transaction.  Used to compare against τ for post-commit execution
  // eligibility in BOF mode.  Returns -1 if the sequence number is not yet
  // recorded locally (e.g., the tx was committed via a cut but this replica
  // hasn't assigned a seq number to it yet).
  int64_t GetBofSeq(const std::string& txn_hash);

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
  // Returns batches in topological order. Does NOT mark transactions committed —
  // call MarkBofCommitted() only after the slot is actually committed (2Δ timer).
  std::vector<std::vector<std::string>> GetBatchOrderedTransactions(
      int64_t tau_prev, int64_t tau_current);

  // Mark transactions as committed in the BOF system.
  // Must be called from Commit() (not at proposal time) so that transactions
  // from a failed/skipped slot are not permanently lost.
  void MarkBofCommitted(const std::vector<std::string>& hashes);

  // OL mode: remove committed transactions from ts_store_, local_ordering_keys_,
  // and committed_keys_ so those maps don't grow without bound across slots.
  // Without this, ComputeAllOrderingKeys() and GetTransactionsInWindow() iterate
  // over every transaction ever received — O(total_ever_seen) per slot.
  void PruneOlCommitted(const std::vector<std::string>& hashes);

  // Read-only variant: same computation as GetBatchOrderedTransactions but does
  // NOT mark transactions as committed. Used by replicas to validate a leader's
  // BOF proposal payload (Algorithm 3) without mutating state prematurely.
  std::vector<std::vector<std::string>> ComputeBatchOrderingReadOnly(
      int64_t tau_prev, int64_t tau_current);

  // Set whether batch-order fairness mode is enabled.
  void SetBatchOrderFairness(bool enabled) { batch_order_fairness_ = enabled; }
  bool IsBatchOrderFairness() const { return batch_order_fairness_; }

  // Set the γ parameter for BOF (edge threshold θ = ⌈γ(f+1)⌉).
  // γ ∈ (0.5, 1.0]; default 1.0 (= f+1, strictest, works for n=2f+1).
  void SetBofGamma(float gamma) { bof_gamma_ = gamma; }

  // Public so the free-function helper ComputeBatchOrderingImpl (defined in
  // the .cpp) can reference it.  Holds up to f+1 orderings for a given block,
  // each ordering being the list of txn hashes in one replica's observed
  // receive order.  See block_orderings_ below for the storage container.
  struct BlockOrderings {
    std::vector<std::vector<std::string>> orderings;
    std::unordered_set<int> contributors;
  };

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

  // Returns candidate transactions for BOF batch computation: those in the
  // window (tau_prev, tau_current] that are not yet in bof_committed_txns_.
  // Used by GetBatchOrderedTransactions, ComputeBatchOrderingReadOnly.
  std::vector<std::string> CollectBofCandidates(int64_t tau_prev, int64_t tau_current);

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

  std::mutex bof_mutex_;

  // Per-block stored orderings.  Each ordering is the list of txn hashes in
  // one replica's local receive order for a particular block.  Keyed by
  // (block_sender_id, block_local_id); each entry holds at most f+1 orderings
  // (the cap enforced by Algorithm 5's "first f+1 blocks with attestations").
  // Storing orderings as lists avoids the O(k²) pair expansion previously
  // performed on every incoming BOF_RelativeOrder message under bof_mutex_,
  // which starved the network-message thread pool and caused BlockACK
  // processing to stall at moderate input rates.
  std::map<std::pair<int, int64_t>, BlockOrderings> block_orderings_;

  // Reverse index: txn_hash → the (block_sender, block_id) where it appears.
  // Txns never migrate between blocks; populated on first AddRelativeOrdering
  // for a given block and pruned when the txn is committed.
  std::unordered_map<std::string, std::pair<int, int64_t>> txn_to_block_;

  // Deduplication: blocks for which we have already recorded a relative
  // ordering (keyed by "sender_id_block_sender_id_block_local_id").
  std::unordered_set<std::string> bof_seen_blocks_;

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
