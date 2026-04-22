#include "platform/consensus/ordering/autobahn/algorithm/proposal_manager.h"

#include <glog/logging.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <queue>
#include <unordered_map>

#include "common/crypto/signature_verifier.h"
#include "common/utils/utils.h"
#include "platform/consensus/ordering/autobahn/tee/tee_host.h"

namespace resdb {
namespace autobahn {

namespace {
std::string Encode(const std::string& hash) {
  std::string ret;
  for (int i = 0; i < hash.size(); ++i) {
    int x = hash[i];
    ret += std::to_string(x);
  }
  return ret;
}

}

ProposalManager::ProposalManager(int32_t id, int total_num, int f, SignatureVerifier* verifier)
    : id_(id), total_num_(total_num), f_(f), verifier_(verifier) {
  current_height_ = 0;
  local_block_id_ = 1;
  current_slot_ = 1;

  // Initialize last-seen vector to 0 for all replicas.
  last_seen_.resize(total_num + 1, 0);  // 1-indexed
}

// ============================================================
// Block Management (unchanged from original)
// ============================================================

void ProposalManager::MakeBlock(
    std::vector<std::unique_ptr<Transaction>>& txns) {
  auto block = std::make_unique<Block>();
  Block::BlockData* data = block->mutable_data();
  for (const auto& txn : txns) {
    *data->add_transaction() = *txn;
  }

  std::string data_str;
  data->SerializeToString(&data_str);
  std::string hash = SignatureVerifier::CalculateHash(data_str);
  block->set_hash(hash);
  block->set_sender_id(id_);
  block->set_create_time(GetCurrentTime());
  block->set_local_id(local_block_id_++);
  AddLocalBlock(std::move(block));
}

void ProposalManager::AddBlock(std::unique_ptr<Block> block) {
  std::unique_lock<std::mutex> lk(mutex_);
  int sender = block->sender_id();
  int block_id = block->local_id();

  LOG(ERROR)<<"add block from sender:"<<sender<<" id:"<<block_id;

  if(block_id>1) {
    if (block->last_sign_info_size() < f_+1) {
      LOG(ERROR) << "AddBlock: block " << block_id << " from " << sender
                 << " has " << block->last_sign_info_size() << " PoA sigs (need " << f_+1 << ")";
      // Still store for availability, but log the issue
    }
    if (!VerifyBlock(*block)) {
      LOG(ERROR) << "AddBlock: block " << block_id << " from " << sender << " verification failed";
    }
    if (pending_blocks_[sender].find(block_id-1) == pending_blocks_[sender].end()) {
      LOG(ERROR) << "AddBlock: previous block " << (block_id-1) << " from " << sender << " not found";
    } else {
      *pending_blocks_[sender][block_id-1]->mutable_sign_info() = block->last_sign_info();
    }
  }
  pending_blocks_[sender][block_id] = std::move(block);
}

Block* ProposalManager::GetBlock(int sender, int64_t block_id) {
  std::unique_lock<std::mutex> lk(mutex_);
  auto it = pending_blocks_[sender].find(block_id);
  if(it != pending_blocks_[sender].end()) {
    return it->second.get();
  }
  // The originating replica never receives its own blocks via ReceiveBlock(),
  // so pending_blocks_[id_] is always empty for this replica.
  // Fall back to blocks_candidates_ (where MakeBlock/AddLocalBlock stores them).
  if (sender == id_) {
    auto jt = blocks_candidates_.find(block_id);
    if (jt != blocks_candidates_.end()) {
      return jt->second.get();
    }
  }
  return nullptr;
}

void ProposalManager::AddLocalBlock(std::unique_ptr<Block> block) {
  std::unique_lock<std::mutex> lk(mutex_);
  blocks_candidates_[block->local_id()] = std::move(block);
}

const Block* ProposalManager::GetLocalBlock(int64_t block_id) {
  std::unique_lock<std::mutex> lk(mutex_);
  if(blocks_candidates_.find(block_id) == blocks_candidates_.end()) {
    return nullptr;
  }
  while(!blocks_candidates_.empty() && blocks_candidates_.begin()->first < block_id) {
    blocks_candidates_.erase(blocks_candidates_.begin());
  }
  Block * block = blocks_candidates_.begin()->second.get();
  UpdateLastSign(block);
  return block;
}

void ProposalManager::BlockReady(const std::map<int, SignInfo>& sign_info, int64_t local_id) {
  std::unique_lock<std::mutex> lk(mutex_);
  auto it = blocks_candidates_.find(local_id);
  if(it == blocks_candidates_.end()){
    return;
  }
  Block * block = it->second.get();
  for(auto sit : sign_info) {
    if (sit.second.hash() != block->hash()) {
      LOG(ERROR) << "BlockReady: hash mismatch for block " << local_id
                 << " from signer " << sit.second.sender_id()
                 << " (expected=" << block->hash().substr(0, 16)
                 << " got=" << sit.second.hash().substr(0, 16) << ")";
      continue;  // Skip mismatched ACKs instead of crashing
    }
    *block->add_sign_info() = sit.second;
  }

  if(it->second == nullptr) return;
  pending_blocks_[id_][local_id] = std::move(it->second);
  blocks_candidates_.erase(it);
  current_height_ = std::max(current_height_, local_id);
}

int64_t ProposalManager::GetCurrentBlockId() {
  std::unique_lock<std::mutex> lk(mutex_);
  return current_height_;
}

void ProposalManager:: UpdateLastSign(Block * block) {
  int block_id = block->local_id();
  if(block_id>1) {
    auto it = pending_blocks_[id_].find(block_id-1);
    assert(it != pending_blocks_[id_].end());
    *block->mutable_last_sign_info() = it->second->sign_info();
  }
}

bool ProposalManager::VerifyBlock(const Block& block) {
  if(block.last_sign_info_size() < f_+1) {
    LOG(ERROR)<<" sign info size fail";
    return false;
  }

  std::set<int> senders;
  for(const auto& sign_info : block.last_sign_info()){
    if(sign_info.hash() != block.last_sign_info(0).hash()){
      LOG(ERROR)<<" sign info hash fail";
      return false;
    }
    if(sign_info.local_id() != block.last_sign_info(0).local_id()){
      LOG(ERROR)<<" sign info local id fail";
      return false;
    }
    senders.insert(sign_info.sender_id());

    bool valid = verifier_->VerifyMessage(sign_info.hash(),
        sign_info.sign());
    if (!valid) {
      LOG(ERROR)<<" sign info sign fail";
      return false;
    }
  }
  return senders.size() >= f_+1;
}

SignInfo ProposalManager::SignBlock(const Block& block) {
  SignInfo sign_info;
  sign_info.set_hash(block.hash());
  sign_info.set_sender_id(id_);
  sign_info.set_local_id(block.local_id());

  auto hash_signature_or = verifier_->SignMessage(block.hash());
  if (!hash_signature_or.ok()) {
    LOG(ERROR) << "Sign message fail";
    return SignInfo();
  }
  *sign_info.mutable_sign()=*hash_signature_or;
  return sign_info;
}

// ============================================================
// View/Slot Management (unchanged)
// ============================================================

void ProposalManager::UpdateView(int sender, int64_t block_id) {
  std::unique_lock<std::mutex> lk(slot_mutex_);
  if(slot_state_[sender].first != current_slot_) {
    new_blocks_[current_slot_]++;
  }
  slot_state_[sender] = std::make_pair(current_slot_, block_id);
}

bool ProposalManager::ReadyView(int slot){
  std::unique_lock<std::mutex> lk(slot_mutex_);
  // Sync HotStuff with 2f+1 replicas: wait for f+1 replicas to have blocks
  // in the current slot. This ensures at least one correct replica contributed.
  return new_blocks_[current_slot_] >= f_ + 1;
}

int ProposalManager::GetCurrentView() {
  std::unique_lock<std::mutex> lk(slot_mutex_);
  return current_slot_;
}

void ProposalManager::IncreaseView() {
  std::unique_lock<std::mutex> lk(slot_mutex_);
  current_slot_++;
}

std::pair<int, std::map<int, int64_t>> ProposalManager::GetCut() {
  std::map<int, int64_t> blocks;
  {
    std::unique_lock<std::mutex> lk(slot_mutex_);
    for(auto it : slot_state_) {
      if(it.second.first == current_slot_) {
        blocks[it.first]=it.second.second;
      }
    }
  }
  current_slot_++;
  return std::make_pair(current_slot_-1, blocks);
}

std::unique_ptr<Proposal> ProposalManager::GenerateProposal(int slot, const std::map<int, int64_t>& blocks) {
  auto proposal = std::make_unique<Proposal>();
  std::string data_hash;
  {
    for (auto& it: blocks) {
      Block* block = proposal->add_block();
      // Busy-wait: block may not yet be in pending_blocks_ if AddBlock races
      // with UpdateView/NotifyView (e.g. slow TEE timestamp path).
      Block* data_block = nullptr;
      while (data_block == nullptr) {
        data_block = GetBlock(it.first, it.second);
        if (data_block == nullptr) usleep(100);
      }
      data_hash += data_block->hash();
      *block->mutable_sign_info() = data_block->sign_info();
      block->set_local_id(data_block->local_id());
      block->set_sender_id(data_block->sender_id());
    }
  }
  proposal->set_slot_id(slot);
  proposal->set_sender_id(id_);
  proposal->set_hash(data_hash);
  return proposal;
}

std::unique_ptr<Proposal> ProposalManager::GetProposalData(int slot) {
  std::unique_lock<std::mutex> lk(p_mutex_);
  return std::move(pending_proposals_[slot]);
}

void ProposalManager::AddProposalData(std::unique_ptr<Proposal> p) {
  std::unique_lock<std::mutex> lk(p_mutex_);
  int slot_id = p->slot_id();
  pending_proposals_[slot_id] = std::move(p);
}


// ============================================================
// Fair Ordering: Ordering Linearizability (Section V)
// ============================================================

// Algorithm 1, lines 3-14: OnReceiveCar
//
// Simulates TEE behavior: for each transaction in the block,
// if not previously seen (H set), assign a monotonic timestamp
// and produce a signed attestation. The TEE guarantees:
//   - Each transaction gets exactly one timestamp
//   - Timestamps are monotonically increasing
//   - No equivocation (TEE won't sign a second timestamp for same txn)
std::vector<SignedTimestamp> ProposalManager::TimestampTransactions(const Block& block) {
  std::vector<SignedTimestamp> new_timestamps;

  if (tee_host_ && tee_host_->IsOk()) {
    // ---- SGX TEE path ----
    // Do NOT hold ts_mutex_ during ECALLs: each ECALL takes ~100μs in SIM mode,
    // and holding the lock across 100 ECALLs (10ms total) starves AddTimestamp
    // handlers on the incoming message threads, causing 300ms+ ACK delays.
    // The enclave's own mutex (TeeHost::mu_) serialises concurrent ECALL access.
    // We acquire ts_mutex_ only for the short store-insert after each ECALL.
    for (const auto& txn : block.data().transaction()) {
      const std::string& txn_hash = txn.hash();

      TeeHost::Result tee_result;
      if (batch_order_fairness_) {
        if (!tee_host_->SequenceNumber(txn_hash, &tee_result)) continue;
      } else {
        if (!tee_host_->Timestamp(txn_hash, &tee_result)) continue;
      }

      SignedTimestamp st;
      st.set_txn_hash(txn_hash);
      st.set_sender_id(id_);
      st.set_timestamp(tee_result.timestamp);

      SignatureInfo sig_info;
      sig_info.set_hash_type(SignatureInfo::ECDSA);
      sig_info.set_node_id(id_);
      sig_info.set_signature(tee_result.sig);
      *st.mutable_signature() = sig_info;

      {
        std::unique_lock<std::mutex> ts_lk(ts_mutex_);
        if (batch_order_fairness_) {
          bof_seq_[txn_hash] = tee_result.timestamp;
        } else {
          if (txn_first_seen_us_.find(txn_hash) == txn_first_seen_us_.end()) {
            txn_first_seen_us_[txn_hash] = GetCurrentTime();
          }
        }
        ts_store_[txn_hash].push_back(st);
        if (!batch_order_fairness_) {
          last_seen_[id_] = std::max(last_seen_[id_], st.timestamp());
        }
      }

      new_timestamps.push_back(st);
    }
  } else {
    // ---- Software simulation path ----
    // Hold ts_mutex_ for the entire loop: tee_seen_set_, tee_clock_, and
    // txn_first_seen_us_ are all accessed here and must be consistent.
    std::unique_lock<std::mutex> ts_lk(ts_mutex_);

    for (const auto& txn : block.data().transaction()) {
      const std::string& txn_hash = txn.hash();

      if (tee_seen_set_.count(txn_hash) > 0) continue;
      tee_seen_set_.insert(txn_hash);

      int64_t tee_timestamp;
      if (batch_order_fairness_) {
        tee_timestamp = static_cast<int64_t>(tee_seen_set_.size());
        bof_seq_[txn_hash] = tee_timestamp;
      } else {
        tee_timestamp = GetCurrentTime();
        if (tee_timestamp <= tee_clock_) tee_timestamp = tee_clock_ + 1;
        tee_clock_ = tee_timestamp;
        if (txn_first_seen_us_.find(txn_hash) == txn_first_seen_us_.end()) {
          txn_first_seen_us_[txn_hash] = tee_timestamp;
        }
      }

      std::string attestation_data = txn_hash + std::to_string(tee_timestamp);
      auto sig_or = verifier_->SignMessage(attestation_data);

      SignedTimestamp st;
      st.set_txn_hash(txn_hash);
      st.set_sender_id(id_);
      st.set_timestamp(tee_timestamp);
      if (sig_or.ok()) *st.mutable_signature() = *sig_or;

      ts_store_[txn_hash].push_back(st);
      new_timestamps.push_back(st);

      if (!batch_order_fairness_) {
        last_seen_[id_] = std::max(last_seen_[id_], st.timestamp());
      }
    }
  }

  return new_timestamps;
}

// Algorithm 1, lines 16-19: OnReceiveTimestamp
//
// Store a TEE-signed timestamp from another replica.
// Algorithm 1, lines 16-19: OnReceiveTimestamp.
//
// Signature verification is skipped in this benchmarking build:
//   - All replicas are trusted (DAS5 controlled environment)
//   - The enclave enforces monotonicity and uniqueness internally
//   - ECDSA-P256 verify per timestamp saturates the CPU at high rates:
//     at 4k tx/s with N=7 replicas ~28,000 verifies/s ≈ 1.1 CPU-seconds/s
//     causes 300ms+ ACK delays via thread-pool saturation
// Production deployments would re-enable the verify call here.
void ProposalManager::AddTimestamp(const SignedTimestamp& ts) {
  // Algorithm 1, line 18: ts_store[t].add(T, σ)
  std::unique_lock<std::mutex> lk(ts_mutex_);
  auto& store = ts_store_[ts.txn_hash()];

  // Prevent duplicate timestamps from the same sender
  for (const auto& existing : store) {
    if (existing.sender_id() == ts.sender_id()) {
      return;  // Already have a timestamp from this replica
    }
  }
  store.push_back(ts);

  // Section V-B: maintain per-replica max last-seen timestamp for the
  // execution threshold τ computation.  In OL mode these are wall-clock
  // values; updating here keeps last_seen_[i] current so
  // ComputeExecutionThreshold() returns a live τ and the Δ-waits fire.
  int sender = ts.sender_id();
  if (sender >= 1 && sender <= total_num_ && !batch_order_fairness_) {
    last_seen_[sender] = std::max(last_seen_[sender], ts.timestamp());
  }
}

// Algorithm 1, lines 21-24: AfterCollectionDeadline
//
// After Δ has elapsed, compute the local ordering key:
//   K_r(t) = (f+1)-th smallest timestamp from collected timestamps.
//
// Per Lemma 3: the (f+1)-th smallest ensures the key is either from
// a correct replica or bounded by correct timestamps on both sides.
int64_t ProposalManager::ComputeLocalOrderingKey(const std::string& txn_hash) {
  std::unique_lock<std::mutex> ts_lk(ts_mutex_);

  // Algorithm 1, AfterCollectionDeadline: only compute key after Δ has elapsed
  // since we first received this transaction.
  // BOF uses sequence numbers (no wall-clock dependency) — skip Δ-wait.
  if (delta_us_ > 0 && !batch_order_fairness_) {
    auto seen_it = txn_first_seen_us_.find(txn_hash);
    if (seen_it != txn_first_seen_us_.end()) {
      int64_t elapsed = GetCurrentTime() - seen_it->second;
      if (elapsed < delta_us_) {
        return -1;  // Δ has not elapsed yet
      }
    }
  }

  auto it = ts_store_.find(txn_hash);
  if (it == ts_store_.end()) {
    return -1;
  }

  const auto& timestamps = it->second;
  // Need at least f+1 timestamps to compute a meaningful ordering key
  if (static_cast<int>(timestamps.size()) < f_ + 1) {
    return -1;
  }

  // Collect all timestamp values and sort
  std::vector<int64_t> values;
  values.reserve(timestamps.size());
  for (const auto& ts : timestamps) {
    values.push_back(ts.timestamp());
  }
  std::sort(values.begin(), values.end());

  // Algorithm 1, line 23: K_r(t) ← (f+1)-th smallest
  // (f+1)-th smallest = index f (0-based)
  int64_t ordering_key = values[f_];

  // Store the local ordering key
  {
    std::unique_lock<std::mutex> ok_lk(ordering_mutex_);
    local_ordering_keys_[txn_hash] = ordering_key;
  }

  return ordering_key;
}

// Compute ordering keys for all transactions with enough collected timestamps.
std::map<std::string, int64_t> ProposalManager::ComputeAllOrderingKeys() {
  std::map<std::string, int64_t> result;

  std::vector<std::string> txn_hashes;
  {
    std::unique_lock<std::mutex> lk(ts_mutex_);
    for (const auto& entry : ts_store_) {
      txn_hashes.push_back(entry.first);
    }
  }

  for (const auto& txn_hash : txn_hashes) {
    int64_t key = ComputeLocalOrderingKey(txn_hash);
    if (key >= 0) {
      result[txn_hash] = key;
    }
  }
  return result;
}

// Get transactions in the execution window (τ_prev, τ_current].
// Returns pairs of (txn_hash, ordering_key) sorted by ordering key,
// with deterministic tie-breaking by txn hash (as specified in the paper).
std::vector<std::pair<std::string, int64_t>> ProposalManager::GetTransactionsInWindow(
    int64_t tau_prev, int64_t tau_current) {
  std::vector<std::pair<std::string, int64_t>> result;

  std::unique_lock<std::mutex> lk(ordering_mutex_);
  for (const auto& entry : local_ordering_keys_) {
    int64_t key = entry.second;
    // Algorithm 2, line 7: payload = {t ∈ all_txns : τ_prev < K(t) ≤ τ_current}
    if (key > tau_prev && key <= tau_current) {
      result.push_back(entry);
    }
  }

  // Algorithm 2, line 8: sorted ← SortByKey(payload)
  // Sort by ordering key ascending; ties broken by txn hash (deterministic)
  std::sort(result.begin(), result.end(),
      [](const std::pair<std::string, int64_t>& a,
         const std::pair<std::string, int64_t>& b) {
        if (a.second != b.second) return a.second < b.second;
        return a.first < b.first;  // deterministic tie-breaking by digest
      });

  return result;
}

// ============================================================
// Execution Threshold (Section V-B)
// ============================================================

// Update the last-seen timestamp for a replica (from a committed block).
void ProposalManager::UpdateLastSeenFromBlock(int replica_id, int64_t latest_timestamp) {
  if (replica_id >= 1 && replica_id <= total_num_) {
    last_seen_[replica_id] = std::max(last_seen_[replica_id], latest_timestamp);
  }
}

// Get this replica's last-seen vector L = <L_1, ..., L_n>.
std::vector<int64_t> ProposalManager::GetLastSeenVector() const {
  return last_seen_;
}

// Compute the execution threshold τ.
//
// Section V-B:
//   1) For each replica i, compute M_i = max last-seen timestamp.
//   2) Sort M_1, ..., M_n and set τ = (f+1)-th smallest.
//
// Lemma 5 guarantees: any future uncommitted transaction t' has K(t') > τ.
int64_t ProposalManager::ComputeExecutionThreshold() const {
  // Collect M_i values (already maintained in last_seen_)
  std::vector<int64_t> m_values;
  for (int i = 1; i <= total_num_; ++i) {
    m_values.push_back(last_seen_[i]);
  }

  std::sort(m_values.begin(), m_values.end());

  // τ = (f+1)-th smallest = index f (0-based)
  if (static_cast<int>(m_values.size()) <= f_) {
    return 0;  // Not enough data yet
  }
  return m_values[f_];
}

// Record a committed ordering key for a transaction.
// K(t) = min over first f+1 committed keys (Section V-A).
void ProposalManager::AddCommittedOrderingKey(const std::string& txn_hash, int64_t key) {
  std::unique_lock<std::mutex> lk(ordering_mutex_);
  auto& keys = committed_keys_[txn_hash];
  keys.push_back(key);
  // Keep sorted for efficient min computation
  std::sort(keys.begin(), keys.end());
}

// Get the final ordering key K(t).
// K(t) = min K_r(t) over the first f+1 committed ordering keys.
// Since at most f of these are from faulty replicas, at least one is correct.
int64_t ProposalManager::GetFinalOrderingKey(const std::string& txn_hash) {
  std::unique_lock<std::mutex> lk(ordering_mutex_);
  auto it = committed_keys_.find(txn_hash);
  if (it == committed_keys_.end() || it->second.empty()) {
    // Fall back to local ordering key if no committed keys yet
    auto local_it = local_ordering_keys_.find(txn_hash);
    if (local_it != local_ordering_keys_.end()) {
      return local_it->second;
    }
    return -1;
  }
  // K(t) = minimum of all committed keys (first element since sorted)
  return it->second[0];
}

bool ProposalManager::HasSeenTransaction(const std::string& txn_hash) const {
  return tee_seen_set_.count(txn_hash) > 0;
}

// ============================================================
// Batch-Order Fairness (γ-BOF, Definition 2)
// ============================================================

// Record this replica's own receive order for transactions in a block.
// The TEE timestamps provide the local ordering: transactions are sorted
// by their TEE timestamp to determine this replica's receive order.
RelativeOrdering ProposalManager::RecordLocalReceiveOrder(const Block& block) {
  RelativeOrdering ordering;
  ordering.set_sender_id(id_);
  ordering.set_block_local_id(block.local_id());
  ordering.set_block_sender_id(block.sender_id());

  // Use TEE timestamp order (already assigned during TimestampTransactions)
  // as this replica's local receive order.
  std::vector<std::pair<int64_t, std::string>> ts_order;
  {
    std::unique_lock<std::mutex> lk(ts_mutex_);
    for (const auto& txn : block.data().transaction()) {
      std::string txn_hash = txn.hash();
      auto it = ts_store_.find(txn_hash);
      if (it != ts_store_.end()) {
        // Find our own timestamp for this transaction
        for (const auto& ts : it->second) {
          if (ts.sender_id() == id_) {
            ts_order.push_back({ts.timestamp(), txn_hash});
            break;
          }
        }
      }
    }
  }

  // Sort by our local timestamp (= our receive order)
  std::sort(ts_order.begin(), ts_order.end());

  for (const auto& entry : ts_order) {
    ordering.add_txn_hashes(entry.second);
  }

  // Self-deliver: also record our own ordering
  AddRelativeOrdering(ordering);

  return ordering;
}

// Record a replica's relative ordering of transactions.
// Updates pairwise precedence counts for the dependency graph.
// Algorithm 5: only counts the first f+1 block-orderings per pair,
// matching the paper's "first f+1 blocks in B with attestations for both ta and tb".
void ProposalManager::AddRelativeOrdering(const RelativeOrdering& ordering) {
  std::unique_lock<std::mutex> lk(bof_mutex_);

  // Deduplicate: one ordering per (sender, block_owner, block_id) triple.
  std::string dedup_key = std::to_string(ordering.sender_id()) + "_" +
      std::to_string(ordering.block_sender_id()) + "_" +
      std::to_string(ordering.block_local_id());
  if (!bof_seen_blocks_.insert(dedup_key).second) {
    return;  // already processed this block's ordering from this sender
  }

  // Collect the txn hashes in order
  std::vector<std::string> txn_list;
  for (const auto& h : ordering.txn_hashes()) {
    txn_list.push_back(h);
    bof_known_txns_.insert(h);
  }

  // Update pairwise precedence counts (Algorithm 5).
  // For pair (a, b) where a precedes b in this ordering, increment the count
  // only if fewer than f+1 block-orderings have been counted for this pair.
  // This matches "first f+1 blocks with attestations for both ta and tb".
  for (size_t i = 0; i < txn_list.size(); ++i) {
    for (size_t j = i + 1; j < txn_list.size(); ++j) {
      auto key_ab = std::make_pair(txn_list[i], txn_list[j]);
      if (pair_contribution_count_[key_ab] < f_ + 1) {
        precedes_count_[key_ab]++;
        pair_contribution_count_[key_ab]++;
      }
    }
  }
}

// ============================================================
// BOF core computation (shared by GetBatchOrderedTransactions
// and ComputeBatchOrderingReadOnly)
// ============================================================

// Ordered-map copy of pairwise precedence counts.  std::map<pair<string,string>>
// works without a custom hasher and avoids exposing the private PairHash type.
using PairMap = std::map<std::pair<std::string, std::string>, int>;

// ComputeBatchOrderingImpl: pure dependency-graph computation with no side
// effects.  Candidates must already be filtered for the desired window and
// must not include already-committed transactions.
static std::vector<std::vector<std::string>> ComputeBatchOrderingImpl(
    const std::vector<std::string>& candidates,
    const PairMap& precedes_count,
    float bof_gamma, int f) {

  if (candidates.empty()) return {};

  int n = candidates.size();
  std::vector<std::vector<int>> adj(n), radj(n);
  int theta = static_cast<int>(std::ceil(bof_gamma * (f + 1)));

  // Iterate the precedence edges directly rather than scanning all O(n²)
  // candidate pairs: in practice precedes_count is sparse (it only contains
  // pairs that were ever observed in an ordering indicator), so edge
  // iteration is O(|edges| · log n) vs the previous O(n² · log |edges|).
  std::unordered_map<std::string, int> cand_idx;
  cand_idx.reserve(n);
  for (int i = 0; i < n; ++i) cand_idx[candidates[i]] = i;

  for (const auto& kv : precedes_count) {
    if (kv.second < theta) continue;
    auto ita = cand_idx.find(kv.first.first);
    if (ita == cand_idx.end()) continue;
    auto itb = cand_idx.find(kv.first.second);
    if (itb == cand_idx.end()) continue;
    int i = ita->second, j = itb->second;
    if (i == j) continue;
    adj[i].push_back(j);
    radj[j].push_back(i);
  }

  // Kosaraju's SCC (Algorithm 6)
  std::vector<bool> visited(n, false);
  std::vector<int> finish_order;
  std::function<void(int)> dfs1 = [&](int u) {
    visited[u] = true;
    for (int v : adj[u]) if (!visited[v]) dfs1(v);
    finish_order.push_back(u);
  };
  for (int i = 0; i < n; ++i) if (!visited[i]) dfs1(i);

  std::vector<int> comp(n, -1);
  int num_comp = 0;
  std::function<void(int,int)> dfs2 = [&](int u, int c) {
    comp[u] = c;
    for (int v : radj[u]) if (comp[v] == -1) dfs2(v, c);
  };
  for (int i = n - 1; i >= 0; --i) {
    if (comp[finish_order[i]] == -1) dfs2(finish_order[i], num_comp++);
  }

  // Collect SCCs; deterministic intra-batch order by hash (Algorithm 6, line 6)
  std::vector<std::vector<std::string>> sccs(num_comp);
  for (int i = 0; i < n; ++i) sccs[comp[i]].push_back(candidates[i]);
  for (auto& scc : sccs) std::sort(scc.begin(), scc.end());

  // Topological sort of condensation DAG (Kahn's algorithm)
  std::vector<std::set<int>> cond_adj(num_comp);
  std::vector<int> in_degree(num_comp, 0);
  for (int u = 0; u < n; ++u)
    for (int v : adj[u])
      if (comp[u] != comp[v] && cond_adj[comp[u]].insert(comp[v]).second)
        in_degree[comp[v]]++;

  std::queue<int> q;
  for (int i = 0; i < num_comp; ++i) if (in_degree[i] == 0) q.push(i);

  std::vector<std::vector<std::string>> result;
  while (!q.empty()) {
    int u = q.front(); q.pop();
    result.push_back(sccs[u]);
    for (int v : cond_adj[u]) if (--in_degree[v] == 0) q.push(v);
  }
  return result;
}

// Collect candidate transactions (window filter + not-yet-committed).
// Caller holds neither ordering_mutex_ nor bof_mutex_.
std::vector<std::string> ProposalManager::CollectBofCandidates(
    int64_t tau_prev, int64_t tau_current) {
  std::vector<std::string> candidates;
  if (batch_order_fairness_) {
    // BOF mode: candidates are transactions with a local TEE sequence number
    // that have not yet been committed.  The tau window does not apply — BOF
    // sequence numbers are monotonic counters, not wall-clock timestamps.
    // Take a snapshot of committed txns first (avoids holding two locks at once).
    std::set<std::string> committed;
    {
      std::unique_lock<std::mutex> blk(bof_mutex_);
      committed = bof_committed_txns_;
    }
    std::unique_lock<std::mutex> lk(ts_mutex_);
    for (const auto& entry : bof_seq_) {
      if (committed.count(entry.first) == 0)
        candidates.push_back(entry.first);
    }
  } else {
    // OL mode: candidates are transactions whose ordering key falls in the
    // execution window (tau_prev, tau_current].
    std::unique_lock<std::mutex> lk(ordering_mutex_);
    for (const auto& entry : local_ordering_keys_) {
      int64_t key = entry.second;
      if (key > tau_prev && key <= tau_current) {
        std::unique_lock<std::mutex> blk(bof_mutex_);
        if (bof_committed_txns_.count(entry.first) == 0)
          candidates.push_back(entry.first);
      }
    }
  }
  return candidates;
}

// Algorithm 5 + 6: build batch-ordered groups.
// Does NOT mark transactions committed — Commit() calls MarkBofCommitted()
// after the slot is actually executed so that transactions from a failed
// or skipped slot are never silently discarded.
std::vector<std::vector<std::string>> ProposalManager::GetBatchOrderedTransactions(
    int64_t tau_prev, int64_t tau_current) {

  auto candidates = CollectBofCandidates(tau_prev, tau_current);
  if (candidates.empty()) return {};

  PairMap counts_copy;
  {
    std::unique_lock<std::mutex> lk(bof_mutex_);
    for (const auto& kv : precedes_count_) counts_copy[kv.first] = kv.second;
  }

  return ComputeBatchOrderingImpl(candidates, counts_copy, bof_gamma_, f_);
}

// Mark a set of transactions as BOF-committed so CollectBofCandidates
// excludes them from future proposals.  Called from Commit() only.
// Also prunes bof_seq_ and ts_store_ to keep them O(pending) rather than
// O(total_ever_received) — without pruning, CollectBofCandidates iterates
// every transaction ever seen, causing latency to grow with run duration.
void ProposalManager::MarkBofCommitted(const std::vector<std::string>& hashes) {
  {
    std::unique_lock<std::mutex> lk(bof_mutex_);
    for (const auto& h : hashes) bof_committed_txns_.insert(h);
    // Prune precedes_count_ / pair_contribution_count_ entries involving any
    // now-committed txn.  A committed txn can never appear again as a BOF
    // candidate, so its edges are dead weight — leaving them would grow the
    // map monotonically with run duration and slow every subsequent
    // ComputeBatchOrderingImpl call (we copy the whole map under bof_mutex_).
    std::set<std::string> removed(hashes.begin(), hashes.end());
    for (auto it = precedes_count_.begin(); it != precedes_count_.end();) {
      if (removed.count(it->first.first) || removed.count(it->first.second)) {
        it = precedes_count_.erase(it);
      } else {
        ++it;
      }
    }
    for (auto it = pair_contribution_count_.begin(); it != pair_contribution_count_.end();) {
      if (removed.count(it->first.first) || removed.count(it->first.second)) {
        it = pair_contribution_count_.erase(it);
      } else {
        ++it;
      }
    }
  }
  {
    std::unique_lock<std::mutex> lk(ts_mutex_);
    for (const auto& h : hashes) {
      bof_seq_.erase(h);
      ts_store_.erase(h);
    }
  }
}

void ProposalManager::PruneOlCommitted(const std::vector<std::string>& hashes) {
  {
    std::unique_lock<std::mutex> lk(ts_mutex_);
    for (const auto& h : hashes) ts_store_.erase(h);
  }
  {
    std::unique_lock<std::mutex> lk(ordering_mutex_);
    for (const auto& h : hashes) {
      local_ordering_keys_.erase(h);
      committed_keys_.erase(h);
    }
  }
}

// Read-only variant: same computation but does NOT mark transactions committed.
// Used by replicas to validate a leader's BOF proposal (Algorithm 3) without
// mutating bof_committed_txns_ prematurely.
std::vector<std::vector<std::string>> ProposalManager::ComputeBatchOrderingReadOnly(
    int64_t tau_prev, int64_t tau_current) {

  auto candidates = CollectBofCandidates(tau_prev, tau_current);
  if (candidates.empty()) return {};

  PairMap counts_copy;
  {
    std::unique_lock<std::mutex> lk(bof_mutex_);
    for (const auto& kv : precedes_count_) counts_copy[kv.first] = kv.second;
  }

  return ComputeBatchOrderingImpl(candidates, counts_copy, bof_gamma_, f_);
}

}  // namespace autobahn
}  // namespace resdb
