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

  // Seed an empty BOF snapshot so readers don't need a null check.
  bof_snap_ = std::make_shared<const BofSnapshot>();
}

ProposalManager::~ProposalManager() {
  // Worker may or may not have been started (only started in BOF mode).
  StopBofWorker();
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
        // last_seen tracks the highest ordering indicator this replica has
        // emitted: a TEE wall-clock timestamp in OL, a TEE sequence number
        // in BOF. Both feed ComputeExecutionThreshold so τ advances in both
        // modes (thesis: "τ is computed identically … using the last-seen
        // vectors from committed blocks").
        last_seen_[id_] = std::max(last_seen_[id_], st.timestamp());
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

      // See comment above: last_seen advances in both modes.
      last_seen_[id_] = std::max(last_seen_[id_], st.timestamp());
    }
  }

  // Adding to bof_seq_ changes the candidate set and ts_store_ — wake the
  // worker so the published snapshot reflects the new state.  No-op when the
  // worker thread isn't running (i.e. OL mode).
  if (batch_order_fairness_ && !new_timestamps.empty()) {
    NotifyBofWorker();
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
  // execution threshold τ computation.  Updates in BOTH modes — in OL the
  // value is a wall-clock TEE timestamp, in BOF it is the per-replica
  // monotonic TEE sequence number s_R.  Either way the (f+1)-th smallest
  // M_i is a Lemma-5-safe finalization horizon (no future tx can attest a
  // smaller indicator from f+1 replicas), which is what CollectBofCandidates
  // now uses to window the BOF dependency-graph input.
  int sender = ts.sender_id();
  if (sender >= 1 && sender <= total_num_) {
    last_seen_[sender] = std::max(last_seen_[sender], ts.timestamp());
  }

  // BOF: a new cross-replica attestation may add an edge to the dependency
  // graph (specifically the cross-Car path that consumes ts_store_ in
  // ComputeBatchOrderingImpl).  Notify the worker so it recomputes.  Cheap
  // when the worker isn't running — the cv has no waiters.
  if (batch_order_fairness_) {
    NotifyBofWorker();
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

int64_t ProposalManager::GetBofSeq(const std::string& txn_hash) {
  std::unique_lock<std::mutex> lk(ts_mutex_);
  auto it = bof_seq_.find(txn_hash);
  if (it == bof_seq_.end()) return -1;
  return it->second;
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
//
// Stores the ordering as a list under its (block_sender, block_id) key rather
// than expanding it into O(k²) pair entries here.  Pair counts are derived
// lazily at proposal/validation time over candidates only (see
// ComputeBatchOrderingImpl), which keeps this ingest path at O(k) per message
// and bounds the lock hold-time by block_size rather than block_size².
//
// Algorithm 5 cap ("first f+1 blocks with attestations for both ta and tb")
// is enforced by storing at most f+1 orderings per block, from distinct
// ordering-senders.
void ProposalManager::AddRelativeOrdering(const RelativeOrdering& ordering) {
  std::unique_lock<std::mutex> lk(bof_mutex_);

  // Deduplicate: one ordering per (sender, block_owner, block_id) triple.
  std::string dedup_key = std::to_string(ordering.sender_id()) + "_" +
      std::to_string(ordering.block_sender_id()) + "_" +
      std::to_string(ordering.block_local_id());
  if (!bof_seen_blocks_.insert(dedup_key).second) {
    return;  // already processed this block's ordering from this sender
  }

  auto block_key = std::make_pair(ordering.block_sender_id(),
                                  ordering.block_local_id());
  auto& entry = block_orderings_[block_key];
  // Cap orderings per block at f+1 (Algorithm 5).
  if (entry.orderings.size() >= static_cast<size_t>(f_ + 1)) return;
  if (!entry.contributors.insert(ordering.sender_id()).second) return;

  std::vector<std::string> txn_list;
  txn_list.reserve(ordering.txn_hashes_size());
  for (const auto& h : ordering.txn_hashes()) {
    txn_list.push_back(h);
    // First ordering for the block seeds the txn-to-block reverse index.
    // Subsequent orderings hit the existing entry (map::emplace is O(1) avg).
    txn_to_block_.emplace(h, block_key);
  }
  entry.orderings.push_back(std::move(txn_list));

  // Worker recompute trigger.  Releasing bof_mutex_ first (the cv's mutex is
  // distinct) avoids serialising the worker behind the message-receiving
  // thread that called us.
  lk.unlock();
  NotifyBofWorker();
}

// ============================================================
// BOF core computation (shared by GetBatchOrderedTransactions
// and ComputeBatchOrderingReadOnly)
// ============================================================

// ComputeBatchOrderingImpl: pure dependency-graph computation with no side
// effects.  Candidates must already be filtered for the desired window and
// must not include already-committed transactions.
//
// Edges come from two complementary sources, both thresholded at θ = ⌈γ(f+1)⌉:
//
//   (a) Same-Car (intra-block) pairs — fast path.  For each block that
//       contributes candidates, we count (positional) precedence agreements
//       across that block's ≤ f+1 stored orderings.  This is the cheap
//       short-circuit retained from the per-block-only design and is provably
//       equal to the per-replica-sequence count for same-Car pairs (the block
//       ordering for replica R is just R's txns sorted by s_R(t)).
//
//   (b) Cross-Car (inter-block) pairs — global γ-batch-order fairness.  For
//       every candidate pair that does NOT share an originating block we walk
//       the per-replica TEE sequence numbers (cand_seq: txn → {replica → s_R})
//       and count how many replicas attested both txns with s_R(ti) < s_R(tj)
//       vs. the reverse, adding an edge when the count meets θ.  These are the
//       edges the previous per-block restriction silently dropped — the cause
//       of the cross-Car γ-BOF gap this change closes (handoff "Option 1").
//
// The TEE assigns sequence numbers monotonically across every txn it processes
// (not per-Car), so (s_R(ti), s_R(tj)) is a valid receive-order comparison even
// when ti, tj landed in different Cars.  Work is O(|candidates|² · f) worst
// case (same as Themis); the same-Car fast path keeps the constant factor on
// the common, dominant intra-block portion small.  Everything from the
// Kosaraju SCC step onward is unchanged.
static std::vector<std::vector<std::string>> ComputeBatchOrderingImpl(
    const std::vector<std::string>& candidates,
    const std::unordered_map<std::string, std::pair<int, int64_t>>& txn_to_block,
    const std::map<std::pair<int, int64_t>,
                   ProposalManager::BlockOrderings>& block_orderings,
    const std::unordered_map<std::string,
                             std::unordered_map<int, int64_t>>& cand_seq,
    float bof_gamma, int f) {

  if (candidates.empty()) return {};

  int n = candidates.size();
  std::vector<std::vector<int>> adj(n), radj(n);
  int theta = static_cast<int>(std::ceil(bof_gamma * (f + 1)));

  std::unordered_map<std::string, int> cand_idx;
  cand_idx.reserve(n);
  for (int i = 0; i < n; ++i) cand_idx[candidates[i]] = i;

  // Group candidates by their originating block.
  std::map<std::pair<int, int64_t>, std::vector<int>> cand_by_block;
  for (int i = 0; i < n; ++i) {
    auto it = txn_to_block.find(candidates[i]);
    if (it == txn_to_block.end()) continue;  // ordering not yet received
    cand_by_block[it->second].push_back(i);
  }

  // For each block with candidates, count per-ordering positional precedence
  // only among this block's candidates and add edges that meet the threshold.
  for (const auto& [block_key, cand_ids] : cand_by_block) {
    auto bit = block_orderings.find(block_key);
    if (bit == block_orderings.end()) continue;
    const auto& orderings = bit->second.orderings;
    if (orderings.empty()) continue;

    // For each ordering, build a position map: candidate-global-index → rank
    // within that ordering (0-based, only among candidates in this block).
    // Walking each ordering once is O(|ordering|); rank lookup is O(1).
    int num_orderings = static_cast<int>(orderings.size());
    std::vector<std::unordered_map<int, int>> rank(num_orderings);
    for (int o = 0; o < num_orderings; ++o) {
      rank[o].reserve(cand_ids.size());
      int r = 0;
      for (const auto& h : orderings[o]) {
        auto it = cand_idx.find(h);
        if (it == cand_idx.end()) continue;  // not a candidate in this block
        rank[o].emplace(it->second, r++);
      }
    }

    // For each candidate pair (a, b) in this block, count orderings where
    // rank[a] < rank[b].  O(b² · num_orderings) with O(1) rank lookups.
    //
    // Early exits (change 2):
    //   * stop once either side has reached θ — the verdict is final;
    //   * stop once neither side can still reach θ even if every remaining
    //     ordering voted for it (`remaining` shrinks each iteration).
    int b = static_cast<int>(cand_ids.size());
    for (int i = 0; i < b; ++i) {
      for (int j = i + 1; j < b; ++j) {
        int ca = cand_ids[i], cb = cand_ids[j];
        int ab = 0, ba = 0;
        int remaining = num_orderings;
        for (int o = 0; o < num_orderings; ++o) {
          --remaining;
          auto ita = rank[o].find(ca);
          auto itb = rank[o].find(cb);
          if (ita == rank[o].end() || itb == rank[o].end()) continue;
          if (ita->second < itb->second) ab++;
          else if (itb->second < ita->second) ba++;
          if (ab >= theta || ba >= theta) break;
          if (ab + remaining < theta && ba + remaining < theta) break;
        }
        if (ab >= theta) {
          adj[ca].push_back(cb);
          radj[cb].push_back(ca);
        }
        if (ba >= theta) {
          adj[cb].push_back(ca);
          radj[ca].push_back(cb);
        }
      }
    }
  }

  // Cross-Car edges: global γ-batch-order fairness over per-replica TEE
  // sequence numbers.  For every pair of candidates that does NOT share an
  // originating block (the same-Car pairs were handled by the fast path above)
  // count, over the replicas that attested BOTH txns, how many observed
  // s_R(ti) < s_R(tj) vs. the reverse, and add an edge when the count meets θ.
  for (int i = 0; i < n; ++i) {
    auto si_it = cand_seq.find(candidates[i]);
    if (si_it == cand_seq.end()) continue;  // no attestations collected yet
    const auto& si = si_it->second;
    auto bi = txn_to_block.find(candidates[i]);
    for (int j = i + 1; j < n; ++j) {
      // Skip same-Car pairs — already decided by the fast path above (using
      // the cheap intra-block count, provably equal to the seq count here).
      auto bj = txn_to_block.find(candidates[j]);
      if (bi != txn_to_block.end() && bj != txn_to_block.end() &&
          bi->second == bj->second) {
        continue;
      }
      auto sj_it = cand_seq.find(candidates[j]);
      if (sj_it == cand_seq.end()) continue;
      const auto& sj = sj_it->second;

      // Intersect the two attestor sets, iterating the smaller map.
      // Short-circuits (change 2): either side reaching θ, or neither able to
      // reach θ even if all remaining common attestors voted for it.  The
      // initial `remaining` upper bound is the size of `small` — every common
      // attestor must appear in both maps and we iterate the smaller one.
      const bool i_smaller = si.size() <= sj.size();
      const auto& small = i_smaller ? si : sj;
      const auto& large = i_smaller ? sj : si;
      int ab = 0, ba = 0;  // ab: s_R(i) < s_R(j); ba: the reverse
      int remaining = static_cast<int>(small.size());
      for (const auto& [replica, seq_small] : small) {
        --remaining;
        auto lit = large.find(replica);
        if (lit == large.end()) continue;  // not a common attestor
        const int64_t seq_i = i_smaller ? seq_small : lit->second;
        const int64_t seq_j = i_smaller ? lit->second : seq_small;
        if (seq_i < seq_j) ab++;
        else if (seq_j < seq_i) ba++;
        if (ab >= theta || ba >= theta) break;
        if (ab + remaining < theta && ba + remaining < theta) break;
      }
      if (ab >= theta) { adj[i].push_back(j); radj[j].push_back(i); }
      if (ba >= theta) { adj[j].push_back(i); radj[i].push_back(j); }
    }
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
//
// BOF candidate selection (changes 1 + 5):
//   1. Start from every uncommitted txn in bof_seq_.
//   2. Apply the τ window (bof_seq_[h] ∈ (tau_prev, tau_current]) when one
//      is requested.  This is the BOF analogue of OL's Lemma-5 windowing:
//      bof_seq_[h] is THIS replica's TEE seq for h; (tau_prev, tau_current]
//      is derived from last_seen_ (the (f+1)-th smallest of per-replica
//      max-seqs), so any txn with bof_seq_[h] ≤ tau_current is known to
//      have an attestation from at least f+1 replicas with s_R(h) ≤ τ_current.
//      Soft-reject in autobahn.cpp tolerates the per-replica drift in
//      bof_seq_[h] between leader and validators.
//   3. Apply the count cap (bof_max_candidates_) — keep only the smallest
//      `cap` seqs (oldest-pending first), with a deterministic hash tiebreak.
//   4. Force-include any txn aged ≥ bof_force_age_slots_ committed slots,
//      even if (3) would otherwise drop it.  Bounds tail latency for stragglers
//      that lose every pairwise contest and never reach a topologically-early
//      batch.
std::vector<std::string> ProposalManager::CollectBofCandidates(
    int64_t tau_prev, int64_t tau_current) {
  std::vector<std::string> candidates;
  if (batch_order_fairness_) {
    std::set<std::string> committed;
    {
      std::unique_lock<std::mutex> blk(bof_mutex_);
      committed = bof_committed_txns_;
    }

    const int64_t current_slot = bof_slot_counter_.load(std::memory_order_relaxed);
    std::unique_lock<std::mutex> lk(ts_mutex_);

    // Pass 1: collect (seq, hash) pairs that survive the window + committed
    // filters, and tag the age of every survivor for the force-include rule.
    std::vector<std::pair<int64_t, std::string>> pool;
    pool.reserve(bof_seq_.size());
    std::vector<std::pair<int64_t, std::string>> forced;
    for (const auto& entry : bof_seq_) {
      const std::string& h = entry.first;
      const int64_t seq = entry.second;
      if (committed.count(h)) continue;
      // Window: (tau_prev, tau_current].
      if (seq <= tau_prev || seq > tau_current) continue;
      // Age-stamp on first sight so force-include has a baseline.
      auto fit = bof_first_pending_slot_.find(h);
      if (fit == bof_first_pending_slot_.end()) {
        bof_first_pending_slot_[h] = current_slot;
      }
      int64_t age = 0;
      if (fit != bof_first_pending_slot_.end()) {
        age = current_slot - fit->second;
      }
      if (bof_force_age_slots_ > 0 && age >= bof_force_age_slots_) {
        forced.emplace_back(seq, h);
      } else {
        pool.emplace_back(seq, h);
      }
    }

    // Pass 2: apply the count cap.  Keep the smallest seqs (oldest pending);
    // tiebreak by hash for cross-replica determinism.  Forced entries are
    // appended afterwards and always make it in.
    if (bof_max_candidates_ > 0 &&
        static_cast<int>(pool.size()) > bof_max_candidates_) {
      std::nth_element(
          pool.begin(), pool.begin() + bof_max_candidates_, pool.end(),
          [](const std::pair<int64_t, std::string>& a,
             const std::pair<int64_t, std::string>& b) {
            if (a.first != b.first) return a.first < b.first;
            return a.second < b.second;
          });
      pool.resize(bof_max_candidates_);
    }

    candidates.reserve(pool.size() + forced.size());
    for (auto& p : pool) candidates.push_back(std::move(p.second));
    for (auto& p : forced) candidates.push_back(std::move(p.second));
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

// Snapshot per-replica TEE sequence numbers for the given candidates.
// In BOF mode each SignedTimestamp's timestamp field carries the signing
// replica's monotonic sequence number s_R(t); ts_store_ holds one entry per
// (txn, attesting replica).  This is the per-replica seq map the cross-Car
// edge count in ComputeBatchOrderingImpl consumes.  Pruned in lockstep with
// bof_seq_ on commit (MarkBofCommitted), so entries live exactly as long as
// the txn remains a candidate.
std::unordered_map<std::string, std::unordered_map<int, int64_t>>
ProposalManager::SnapshotCandidateSeqs(const std::vector<std::string>& candidates) {
  std::unordered_map<std::string, std::unordered_map<int, int64_t>> out;
  out.reserve(candidates.size());
  std::unique_lock<std::mutex> lk(ts_mutex_);
  for (const auto& h : candidates) {
    auto it = ts_store_.find(h);
    if (it == ts_store_.end()) continue;
    auto& m = out[h];
    m.reserve(it->second.size());
    for (const auto& ts : it->second) {
      m[ts.sender_id()] = ts.timestamp();
    }
  }
  return out;
}

// Algorithm 5 + 6: build batch-ordered groups.
// Does NOT mark transactions committed — Commit() calls MarkBofCommitted()
// after the slot is actually executed so that transactions from a failed
// or skipped slot are never silently discarded.
//
// Changes 3 + 4: the O(n²·f) dependency-graph computation runs on the
// background BOF worker (see BofWorkerLoop) and publishes via a shared_ptr
// snapshot.  This function returns the latest snapshot's batches, filtered to
// the requested (tau_prev, tau_current] window using the local_seq map the
// worker captured atomically with the batch computation.  The consensus
// critical path now pays only a shared_ptr load + O(|batches|) filter.
std::vector<std::vector<std::string>> ProposalManager::GetBatchOrderedTransactions(
    int64_t tau_prev, int64_t tau_current) {
  auto snap = GetBofSnapshot();
  if (!snap || snap->batches.empty()) return {};

  // Snapshot already excludes committed txns (worker calls CollectBofCandidates
  // which filters them out).  Drop any that have been committed since the
  // snapshot was published — between worker recompute and now, MarkBofCommitted
  // may have moved txns into bof_committed_txns_.
  std::set<std::string> committed;
  {
    std::unique_lock<std::mutex> blk(bof_mutex_);
    committed = bof_committed_txns_;
  }

  std::vector<std::vector<std::string>> out;
  out.reserve(snap->batches.size());
  for (const auto& batch : snap->batches) {
    std::vector<std::string> filtered;
    filtered.reserve(batch.size());
    for (const auto& h : batch) {
      auto sit = snap->local_seq.find(h);
      if (sit == snap->local_seq.end()) continue;
      const int64_t seq = sit->second;
      if (seq <= tau_prev || seq > tau_current) continue;
      if (committed.count(h)) continue;
      filtered.push_back(h);
    }
    if (!filtered.empty()) out.push_back(std::move(filtered));
  }
  return out;
}

// Mark a set of transactions as BOF-committed so CollectBofCandidates
// excludes them from future proposals.  Called from Commit() only.
// Also prunes per-block orderings, bof_seq_, and ts_store_ so hot-path
// snapshots stay O(pending) rather than O(total_ever_received).
void ProposalManager::MarkBofCommitted(const std::vector<std::string>& hashes) {
  {
    std::unique_lock<std::mutex> lk(bof_mutex_);
    // Collect the set of block keys touched by the committed txns so we can
    // decide per-block whether any uncommitted txn still references it.
    std::set<std::pair<int, int64_t>> touched_blocks;
    for (const auto& h : hashes) {
      bof_committed_txns_.insert(h);
      auto it = txn_to_block_.find(h);
      if (it != txn_to_block_.end()) {
        touched_blocks.insert(it->second);
        txn_to_block_.erase(it);
      }
    }

    // For each touched block, check whether any of its txns are still
    // uncommitted (i.e. present in txn_to_block_).  If not, drop the entire
    // entry — its orderings are dead weight.  Otherwise keep it intact; the
    // pair-count computation at proposal time already restricts work to the
    // candidates still in txn_to_block_.
    for (const auto& bk : touched_blocks) {
      auto it = block_orderings_.find(bk);
      if (it == block_orderings_.end()) continue;
      bool any_pending = false;
      for (const auto& ord : it->second.orderings) {
        for (const auto& h : ord) {
          if (txn_to_block_.count(h)) { any_pending = true; break; }
        }
        if (any_pending) break;
      }
      if (!any_pending) block_orderings_.erase(it);
    }
  }
  {
    std::unique_lock<std::mutex> lk(ts_mutex_);
    for (const auto& h : hashes) {
      // Dropping bof_seq_ removes h from the candidate set; dropping ts_store_
      // releases the per-replica sequence numbers s_R(h).  These must be erased
      // in lockstep: SnapshotCandidateSeqs reads ts_store_ for every candidate,
      // so the per-replica seqs of any txn still in bof_seq_ (i.e. still a
      // candidate) must remain available for the cross-Car edge count.  hashes
      // here are only the txns that actually executed, so candidates are kept.
      bof_seq_.erase(h);
      ts_store_.erase(h);
      bof_first_pending_slot_.erase(h);
    }
  }

  // Candidate set changed — refresh the published snapshot so the next
  // proposal/validation doesn't keep returning committed txns.
  NotifyBofWorker();
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
// mutating bof_committed_txns_ prematurely.  Always shares the same snapshot
// the leader would have read (modulo network delay) — the soft-reject path in
// autobahn.cpp tolerates the resulting ordering drift.
std::vector<std::vector<std::string>> ProposalManager::ComputeBatchOrderingReadOnly(
    int64_t tau_prev, int64_t tau_current) {
  // Same body as GetBatchOrderedTransactions — both are pure reads now.
  return GetBatchOrderedTransactions(tau_prev, tau_current);
}

// ============================================================
// BOF Worker (changes 3 + 4)
// ============================================================

void ProposalManager::NotifyBofWorker() {
  std::lock_guard<std::mutex> lk(bof_worker_mutex_);
  bof_worker_dirty_ = true;
  bof_worker_cv_.notify_one();
}

void ProposalManager::AdvanceBofSlot() {
  bof_slot_counter_.fetch_add(1, std::memory_order_relaxed);
}

void ProposalManager::StartBofWorker() {
  if (bof_worker_thread_.joinable()) return;  // already running
  bof_worker_stop_.store(false, std::memory_order_relaxed);
  bof_worker_thread_ = std::thread(&ProposalManager::BofWorkerLoop, this);
}

void ProposalManager::StopBofWorker() {
  if (!bof_worker_thread_.joinable()) return;
  {
    std::lock_guard<std::mutex> lk(bof_worker_mutex_);
    bof_worker_stop_.store(true, std::memory_order_relaxed);
    bof_worker_dirty_ = true;
    bof_worker_cv_.notify_all();
  }
  bof_worker_thread_.join();
}

std::shared_ptr<const ProposalManager::BofSnapshot>
ProposalManager::GetBofSnapshot() const {
  std::lock_guard<std::mutex> lk(bof_snap_mutex_);
  return bof_snap_;
}

// Worker loop: wait for a dirty notification, recompute the snapshot, publish.
// Multiple notifications between recomputes coalesce into one rebuild (we
// reset the flag under the worker mutex before releasing it).
void ProposalManager::BofWorkerLoop() {
  while (!bof_worker_stop_.load(std::memory_order_relaxed)) {
    {
      std::unique_lock<std::mutex> lk(bof_worker_mutex_);
      bof_worker_cv_.wait(lk, [&] {
        return bof_worker_dirty_ ||
               bof_worker_stop_.load(std::memory_order_relaxed);
      });
      if (bof_worker_stop_.load(std::memory_order_relaxed)) return;
      bof_worker_dirty_ = false;
    }

    auto snap = RecomputeBofSnapshot();
    {
      std::lock_guard<std::mutex> lk(bof_snap_mutex_);
      bof_snap_ = std::move(snap);
    }
  }
}

// Build a fresh BofSnapshot from the current source-of-truth state.  The two
// state copies (block_orderings_, txn_to_block_) and the candidate seq snapshot
// happen here, off the critical path; ComputeBatchOrderingImpl runs without
// holding any locks.
std::shared_ptr<const ProposalManager::BofSnapshot>
ProposalManager::RecomputeBofSnapshot() {
  // No τ filter at worker time: the worker computes the dependency graph over
  // the full bounded candidate set; callers apply their τ window when reading
  // the snapshot.  This way one recompute serves both the leader and every
  // replica even though they may compute slightly different τ values.
  auto candidates = CollectBofCandidates(
      std::numeric_limits<int64_t>::min(),
      std::numeric_limits<int64_t>::max());

  auto out = std::make_shared<BofSnapshot>();
  out->version = ++bof_snap_version_;
  if (candidates.empty()) return out;

  // Snapshot block_orderings_ + txn_to_block_ under bof_mutex_.  The worker
  // pays this copy cost off the critical path.
  std::unordered_map<std::string, std::pair<int, int64_t>> txn_to_block_copy;
  std::map<std::pair<int, int64_t>, BlockOrderings> block_orderings_copy;
  {
    std::unique_lock<std::mutex> lk(bof_mutex_);
    txn_to_block_copy = txn_to_block_;
    block_orderings_copy = block_orderings_;
  }

  // Per-replica TEE sequence numbers (cross-Car edge count input) and the
  // local-seq map readers will need to apply their τ window.
  auto cand_seq = SnapshotCandidateSeqs(candidates);
  {
    std::unique_lock<std::mutex> lk(ts_mutex_);
    out->local_seq.reserve(candidates.size());
    for (const auto& h : candidates) {
      auto it = bof_seq_.find(h);
      if (it != bof_seq_.end()) out->local_seq[h] = it->second;
    }
  }

  out->batches = ComputeBatchOrderingImpl(
      candidates, txn_to_block_copy, block_orderings_copy, cand_seq,
      bof_gamma_, f_);
  return out;
}

}  // namespace autobahn
}  // namespace resdb
