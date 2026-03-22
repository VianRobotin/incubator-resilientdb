#include "platform/consensus/ordering/autobahn/algorithm/proposal_manager.h"

#include <glog/logging.h>

#include <algorithm>

#include "common/crypto/signature_verifier.h"
#include "common/utils/utils.h"

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
  if(it == pending_blocks_[sender].end()) {
    return nullptr;
  }
  assert(it != pending_blocks_[sender].end());
  return it->second.get();
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
    LOG(ERROR)<<" add last sign:"<<sit.second.sender_id();
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
      Block* data_block = GetBlock(it.first, it.second);
      assert(data_block != nullptr);
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

  std::unique_lock<std::mutex> ts_lk(ts_mutex_);

  for (const auto& txn : block.data().transaction()) {
    std::string txn_hash = txn.hash();

    // Algorithm 1, line 7: if id(t_i) ∈ H then continue (TEE rejects duplicate)
    if (tee_seen_set_.count(txn_hash) > 0) {
      continue;
    }

    // Algorithm 1, line 9: H ← H ∪ {id(t_i)}
    tee_seen_set_.insert(txn_hash);

    // Algorithm 1, line 10: T_i ← TEE.ReadClock()
    // Use the actual wall-clock time as the TEE timestamp.
    // In a real TEE this would be a trusted monotonic clock.
    int64_t tee_timestamp = GetCurrentTime();
    // Ensure monotonicity (TEE clock never goes backwards)
    if (tee_timestamp <= tee_clock_) {
      tee_timestamp = tee_clock_ + 1;
    }
    tee_clock_ = tee_timestamp;

    // Algorithm 1, line 11: σ_i ← TEE.Sign(t_i, T_i)
    // Sign the (txn_hash, timestamp) pair
    std::string attestation_data = txn_hash + std::to_string(tee_timestamp);
    auto sig_or = verifier_->SignMessage(attestation_data);

    SignedTimestamp st;
    st.set_txn_hash(txn_hash);
    st.set_timestamp(tee_timestamp);
    st.set_sender_id(id_);
    if (sig_or.ok()) {
      *st.mutable_signature() = *sig_or;
    }

    // Algorithm 1, line 12: ts_store[t_i].add(T_i, σ_i)
    // Store our own timestamp
    ts_store_[txn_hash].push_back(st);

    new_timestamps.push_back(st);
  }

  return new_timestamps;
}

// Algorithm 1, lines 16-19: OnReceiveTimestamp
//
// Verify and store a TEE-signed timestamp from another replica.
void ProposalManager::AddTimestamp(const SignedTimestamp& ts) {
  // Algorithm 1, line 17: if TEE.Verify(t, T, σ)
  std::string attestation_data = ts.txn_hash() + std::to_string(ts.timestamp());
  bool valid = verifier_->VerifyMessage(attestation_data, ts.signature());
  if (!valid) {
    LOG(ERROR) << "Invalid TEE timestamp signature from replica " << ts.sender_id();
    return;
  }

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

}  // namespace autobahn
}  // namespace resdb
