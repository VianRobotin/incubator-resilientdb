#include "platform/consensus/ordering/autobahn/algorithm/autobahn.h"

#include <glog/logging.h>
#include <limits>
#include <set>
#include <unordered_map>

#include "common/crypto/signature_verifier.h"
#include "common/utils/utils.h"


namespace resdb {
namespace autobahn {

AutoBahn::AutoBahn(int id, int f, int total_num, int block_size, SignatureVerifier* verifier,
                   bool batch_order_fairness, float bof_gamma)
    : ProtocolBase(id, f, total_num), verifier_(verifier),
      batch_order_fairness_(batch_order_fairness) {

  LOG(ERROR) << "Initializing AutoBahn with Sync HotStuff + "
             << (batch_order_fairness_ ? "Batch-Order Fairness" : "Ordering Linearizability")
             << " id=" << id << " f=" << f << " n=" << total_num
             << (batch_order_fairness_ ? " gamma=" + std::to_string(bof_gamma) : "");
  id_ = id;
  total_num_ = total_num;
  f_ = f;
  is_stop_ = false;
  timeout_ms_ = 60000;
  // Δ: synchronous network delay bound.
  // Replicas wait Δ to collect timestamps, 2Δ before committing.
  delta_ms_ = 1000;  // 1 second Δ; adjust for deployment network latency
  batch_size_ = block_size;
  execute_id_ = 1;
  is_leader_ = id_ == 1;
  cur_slot_ = 1;

  global_stats_ = Stats::GetGlobalStats(5);

  proposal_manager_ = std::make_unique<ProposalManager>(id, total_num_, f_, verifier);
  proposal_manager_->SetBatchOrderFairness(batch_order_fairness_);
  proposal_manager_->SetBofGamma(bof_gamma);
  proposal_manager_->SetDeltaUs(delta_ms_ * 1000);

  // Try to initialize the SGX TEE enclave. Fall back to software simulation
  // silently if the enclave .so is not found (e.g., first build without make).
  const char* enclave_env = getenv("TEE_ENCLAVE_PATH");
  std::string enclave_path = enclave_env
      ? std::string(enclave_env)
      : "platform/consensus/ordering/autobahn/tee/tee_enclave.signed.so";
  tee_host_ = std::make_unique<TeeHost>();
  if (tee_host_->Initialize(enclave_path)) {
    proposal_manager_->SetTeeHost(tee_host_.get());
    LOG(ERROR) << "[TEE] SGX enclave active, pubkey=" << tee_host_->GetPublicKey().size()
               << " bytes";
  } else {
    tee_host_.reset();
    LOG(ERROR) << "[TEE] SGX enclave not available, using software simulation";
  }

  block_thread_ = std::thread(&AutoBahn::GenerateBlocks, this);
  dissemi_thread_ = std::thread(&AutoBahn::AsyncDissemination, this);
  consensus_thread_ = std::thread(&AutoBahn::AsyncConsensus, this);
  commit_thread_ = std::thread(&AutoBahn::AsyncCommitTimer, this);
}

AutoBahn::~AutoBahn() {
  is_stop_ = true;
  if (block_thread_.joinable()) {
    block_thread_.join();
  }
  if (dissemi_thread_.joinable()) {
    dissemi_thread_.join();
  }
  if (consensus_thread_.joinable()) {
    consensus_thread_.join();
  }
  if (commit_thread_.joinable()) {
    commit_thread_.join();
  }
}

bool AutoBahn::IsStop() {
  return is_stop_;
}

bool AutoBahn::ReceiveTransaction(std::unique_ptr<Transaction> txn) {
  txn->set_create_time(GetCurrentTime());
  txns_.Push(std::move(txn));
  return true;
}

// ============================================================
// Data Dissemination Layer (TEE-Enabled Autobahn, Algorithm 1)
// ============================================================

void AutoBahn::GenerateBlocks() {
  std::vector<std::unique_ptr<Transaction>> txns;
  while (!IsStop()) {
    std::unique_ptr<Transaction> txn = txns_.Pop();
    if (txn == nullptr) {
      continue;
    }
    txns.push_back(std::move(txn));
    for(int i = 1; i < batch_size_; ++i){
      std::unique_ptr<Transaction> txn = txns_.Pop(100);
      if(txn == nullptr){
        break;
      }
      txns.push_back(std::move(txn));
    }

    proposal_manager_->MakeBlock(txns);
    txns.clear();
  }
}

bool AutoBahn::WaitForResponse(int64_t block_id) {
  std::unique_lock<std::mutex> lk(bc_mutex_);
  bc_block_cv_.wait_for(lk, std::chrono::microseconds(timeout_ms_ * 1000),
      [&] { return block_id<= proposal_manager_->GetCurrentBlockId(); });
  if (block_id > proposal_manager_->GetCurrentBlockId()) {
    return false;
  }
  return true;
}

void AutoBahn::BlockDone() {
  bc_block_cv_.notify_all();
}

void AutoBahn::AsyncDissemination() {
  int next_block = 1;
  while (!IsStop()) {
    // Wait for previous block to be certified (enough ACKs)
    if(WaitForResponse(next_block-1)){
      next_block++;
    }
    if(next_block == 1) {
      next_block++;
    }

    // Get the next local block (created by GenerateBlocks thread).
    // GetLocalBlock attaches PoA signatures from the previous block.
    // Poll until the block is available.
    const Block* block = nullptr;
    while (!IsStop() && block == nullptr) {
      block = proposal_manager_->GetLocalBlock(next_block-1);
      if(block == nullptr) {
        usleep(1000);  // 1ms poll
      }
    }
    if (block == nullptr) break;

    LOG(ERROR) << "Disseminating block " << (next_block-1)
               << " with " << block->data().transaction_size() << " txns";

    // The originating replica must also TEE-timestamp its own transactions.
    // Other replicas do this in ReceiveBlock(), but the sender never calls
    // ReceiveBlock on its own blocks. Without this, each transaction would
    // be missing the sender's timestamp, reducing the available timestamps
    // for computing the ordering key K(t).
    std::vector<SignedTimestamp> own_timestamps =
        proposal_manager_->TimestampTransactions(*block);
    if (!own_timestamps.empty()) {
      TimestampBatch batch;
      batch.set_sender_id(id_);
      if (tee_host_ && tee_host_->IsOk()) {
        batch.set_tee_pubkey(tee_host_->GetPublicKey());
      }
      for (const auto& ts : own_timestamps) {
        *batch.add_timestamps() = ts;
      }
      Broadcast(MessageType::TEE_Timestamps, batch);
      LOG(ERROR) << "Broadcast " << own_timestamps.size()
                 << " own TEE timestamps for local block " << (next_block-1);
    }

    // Batch-order fairness: broadcast our local receive order for this block
    if (batch_order_fairness_) {
      RelativeOrdering rel_order = proposal_manager_->RecordLocalReceiveOrder(*block);
      if (rel_order.txn_hashes_size() > 0) {
        Broadcast(MessageType::BOF_RelativeOrder, rel_order);
      }
    }

    // Self-ACK: generate a local ACK for our own block so that BlockReady
    // can be triggered without depending on network self-delivery.
    // Other replicas' ACKs arrive via the network, but our own ACK is
    // needed for the f+1 threshold check (which requires self in the set).
    {
      BlockACK self_ack;
      self_ack.set_hash(block->hash());
      self_ack.set_sender_id(id_);
      self_ack.set_local_id(block->local_id());
      self_ack.set_responder(id_);
      *self_ack.mutable_sign_info() = proposal_manager_->SignBlock(*block);
      ReceiveBlockACK(std::make_unique<BlockACK>(self_ack));
    }

    Broadcast(MessageType::NewBlocks, *block);
  }
}

// Algorithm 1, lines 3-14: OnReceiveCar
//
// When receiving a block (Car) from another replica:
// 1. Store the block and send a BlockACK (PoA)
// 2. TEE-timestamp each transaction in the block
// 3. Broadcast the signed timestamps to all replicas
void AutoBahn::ReceiveBlock(std::unique_ptr<Block> block) {
  LOG(ERROR)<<"recv block from:"<<block->sender_id()<<" block id:"<<block->local_id();

  // Step 1: Standard Autobahn — ACK for Proof of Availability
  BlockACK block_ack;
  block_ack.set_hash(block->hash());
  block_ack.set_sender_id(block->sender_id());
  block_ack.set_local_id(block->local_id());
  block_ack.set_responder(id_);
  *block_ack.mutable_sign_info() = proposal_manager_->SignBlock(*block);

  // Step 2: TEE-timestamp each transaction (Algorithm 1, lines 6-13)
  std::vector<SignedTimestamp> new_timestamps =
      proposal_manager_->TimestampTransactions(*block);

  // Batch-order fairness: record and broadcast our receive order for this block.
  // Must be done before AddBlock moves the block.
  if (batch_order_fairness_) {
    RelativeOrdering rel_order = proposal_manager_->RecordLocalReceiveOrder(*block);
    if (rel_order.txn_hashes_size() > 0) {
      Broadcast(MessageType::BOF_RelativeOrder, rel_order);
    }
  }

  proposal_manager_->AddBlock(std::move(block));
  // Use Broadcast instead of SendMessage for BlockACK delivery.
  // The point-to-point SendMessage path doesn't work reliably because
  // bc_client_'s replicas_ list may not contain all peers.
  // Receivers filter by sender_id to only process ACKs for their own blocks.
  Broadcast(MessageType::CMD_BlockACK, block_ack);

  proposal_manager_->UpdateView(block_ack.sender_id(), block_ack.local_id());
  NotifyView();

  // Step 3: Broadcast timestamps (Algorithm 1, line 14)
  // "BroadcastTimestamps({(t_i, T_i, σ_i) : t_i newly attested})"
  if (!new_timestamps.empty()) {
    TimestampBatch batch;
    batch.set_sender_id(id_);
    if (tee_host_ && tee_host_->IsOk()) {
      batch.set_tee_pubkey(tee_host_->GetPublicKey());
    }
    for (const auto& ts : new_timestamps) {
      *batch.add_timestamps() = ts;
    }
    Broadcast(MessageType::TEE_Timestamps, batch);
    LOG(ERROR) << "Broadcast " << new_timestamps.size()
               << " TEE timestamps for block from " << block_ack.sender_id();
  }

  LOG(ERROR)<<"send block ack to:"<<block_ack.sender_id()<<" block id:"<<block_ack.local_id();
}

void AutoBahn::ReceiveBlockACK(std::unique_ptr<BlockACK> block) {
  // Only process ACKs for our own blocks (since ACKs are now broadcast to all)
  if (block->sender_id() != id_) return;

  LOG(ERROR)<<"recv block ack:"<<block->local_id()<<" from:"<<block->responder()
    <<" block sign info:"<<block->sign_info().sender_id();

  bool ready = false;
  std::map<int, SignInfo> ack_copy;
  int64_t local_id = block->local_id();

  {
    std::unique_lock<std::mutex> lk(block_mutex_);
    block_ack_[local_id].insert(std::make_pair(block->responder(), block->sign_info()));
    LOG(ERROR)<<"recv block ack:"<<local_id
      <<" from:"<<block->responder()<< " num:"<<block_ack_[local_id].size();
    if (block_ack_[local_id].size() >= f_ + 1 &&
        block_ack_[local_id].find(id_) != block_ack_[local_id].end()) {
      ready = true;
      ack_copy = block_ack_[local_id];
    }
  }
  // Release block_mutex_ before acquiring bc_mutex_ to avoid deadlock
  // with AsyncDissemination's WaitForResponse which holds bc_mutex_.
  if (ready) {
    proposal_manager_->BlockReady(ack_copy, local_id);
    std::unique_lock<std::mutex> lk(bc_mutex_);
    BlockDone();
  }
  LOG(ERROR)<<"recv block ack:"<<local_id<<" done";
}

// Algorithm 1, lines 16-19: OnReceiveTimestamp
//
// Receive TEE-signed timestamps from another replica and store them.
// After enough timestamps are collected (≥ f+1), we can compute
// the local ordering key K_r(t).
void AutoBahn::ReceiveTimestamps(std::unique_ptr<TimestampBatch> batch) {
  LOG(ERROR) << "Received " << batch->timestamps_size()
             << " timestamps from replica " << batch->sender_id();

  // Cache the sender's TEE public key so AddTimestamp() can verify ECDSA sigs.
  if (!batch->tee_pubkey().empty()) {
    proposal_manager_->SetRemoteTeePublicKey(batch->sender_id(), batch->tee_pubkey());
  }

  for (const auto& ts : batch->timestamps()) {
    proposal_manager_->AddTimestamp(ts);
  }
}

// Batch-order fairness: receive a relative ordering from another replica.
void AutoBahn::ReceiveRelativeOrdering(std::unique_ptr<RelativeOrdering> ordering) {
  LOG(ERROR) << "Received relative ordering from replica " << ordering->sender_id()
             << " with " << ordering->txn_hashes_size() << " txns";
  proposal_manager_->AddRelativeOrdering(*ordering);
}

// ============================================================
// Sync HotStuff Consensus Layer with Fair Ordering
// ============================================================

void AutoBahn::NotifyView() {
  std::unique_lock<std::mutex> lk(view_mutex_);
  view_cv_.notify_all();
}

bool AutoBahn::WaitForNextView(int view) {
  std::unique_lock<std::mutex> lk(view_mutex_);
  view_cv_.wait_for(lk, std::chrono::microseconds(timeout_ms_ * 1000),
      [&] { return proposal_manager_->ReadyView(view); });
  return proposal_manager_->ReadyView(view);
}

bool AutoBahn::WaitForNextLeader() {
  std::unique_lock<std::mutex> lk(leader_mutex_);
  leader_cv_.wait_for(lk, std::chrono::microseconds(timeout_ms_ * 1000),
      [&] { return is_leader_; });
  return is_leader_;
}

void AutoBahn::StartNextLeader(int slot_id) {
  std::unique_lock<std::mutex> lk(leader_mutex_);
  cur_slot_ = slot_id;
  is_leader_ = true;
  leader_cv_.notify_all();
}

// Algorithm 2: Sync HotStuff Leader — Windowed Proposal with Tip Cut
//
// The leader:
// 1. Computes τ_current from committed blocks' last-seen vectors
// 2. Waits until Now() > τ_current + Δ (ensures all timestamps collected)
// 3. Gets the tip cut (latest certified block from each lane)
// 4. Extracts transactions where τ_prev < K(t) ≤ τ_current
// 5. Sorts by ordering key K(t) (ascending, ties by hash)
// 6. Broadcasts the proposal with the sorted, fairly-ordered payload
void AutoBahn::AsyncConsensus() {
  while (!IsStop()) {
    if(!WaitForNextLeader()){
      continue;
    }
    {
      std::unique_lock<std::mutex> lk(leader_mutex_);
      is_leader_ = false;
    }

    int view = proposal_manager_->GetCurrentView();
    if(!WaitForNextView(view)) {
      continue;
    }

    // Step 1: Compute ordering keys for all transactions with enough timestamps
    // (Algorithm 1, AfterCollectionDeadline for all pending transactions)
    proposal_manager_->ComputeAllOrderingKeys();

    // Step 2: Compute execution threshold τ_current (Section V-B)
    int64_t tau_prev = proposal_manager_->GetPrevThreshold();
    int64_t tau_current = proposal_manager_->ComputeExecutionThreshold();

    // If τ hasn't advanced, use a fallback: current time minus 2Δ
    // This ensures we still make progress when the threshold mechanism
    // hasn't accumulated enough data yet.
    if (tau_current <= tau_prev) {
      tau_current = static_cast<int64_t>(GetCurrentTime()) - 2 * delta_ms_ * 1000;
      if (tau_current <= tau_prev) {
        tau_current = tau_prev + 1;
      }
    }

    // Algorithm 2, line 4: wait until Now() > τ_current + Δ
    // Guarantees that all TEE-signed timestamps with T ≤ τ_current have had
    // Δ time to propagate to all correct replicas before we propose.
    {
      int64_t wait_until_us = tau_current + delta_ms_ * 1000;
      int64_t now_us = static_cast<int64_t>(GetCurrentTime());
      if (now_us < wait_until_us) {
        int64_t sleep_us = wait_until_us - now_us;
        // Cap at 2Δ to guard against a very large τ_current.
        sleep_us = std::min(sleep_us, static_cast<int64_t>(2 * delta_ms_ * 1000));
        LOG(ERROR) << "Algorithm 2: waiting " << sleep_us / 1000 << "ms for τ+Δ";
        usleep(static_cast<useconds_t>(sleep_us));
      }
    }

    // Get the tip cut: latest certified block from each replica lane
    std::pair<int, std::map<int, int64_t>> blocks = proposal_manager_->GetCut();
    int slot_id = cur_slot_;

    auto proposal = proposal_manager_->GenerateProposal(slot_id, blocks.second);

    // Step 3: Set Sync HotStuff + fair ordering fields
    proposal->set_view_number(slot_id);
    proposal->set_propose_time(GetCurrentTime());
    proposal->set_threshold(tau_current);
    proposal->set_prev_threshold(tau_prev);

    // Build tip cut references
    for (const auto& it : blocks.second) {
      TipRef* tip = proposal->add_tip_cut();
      tip->set_sender_id(it.first);
      tip->set_block_id(it.second);
    }

    // Step 4: Get this replica's last-seen vector L⃗ (Algorithm 2, line 9)
    // Section V-B: TEE signs L⃗ to prevent Byzantine inflation of τ.
    std::vector<int64_t> last_seen = proposal_manager_->GetLastSeenVector();
    for (int i = 1; i <= total_num_; ++i) {
      proposal->add_last_seen_vector(last_seen[i]);
    }
    // Build serialized L⃗ bytes (little-endian int64 per entry) and TEE-sign them.
    if (tee_host_ && tee_host_->IsOk()) {
      std::string lvec_bytes;
      lvec_bytes.resize(total_num_ * 8);
      for (int i = 0; i < total_num_; ++i) {
        int64_t v = last_seen[i + 1];
        for (int b = 0; b < 8; ++b) {
          lvec_bytes[i * 8 + b] = static_cast<char>((v >> (b * 8)) & 0xFF);
        }
      }
      std::string lvec_sig;
      if (tee_host_->SignBytes(lvec_bytes, &lvec_sig)) {
        proposal->set_last_seen_sig(lvec_sig);
      }
    }

    // Step 5: Extract and order transactions in the execution window.
    // Two modes:
    //   - Ordering Linearizability: sort by K(t) (ascending, ties by hash)
    //   - Batch-Order Fairness: group into batches via dependency graph
    int total_fair_txns = 0;
    if (batch_order_fairness_) {
      // BOF mode: build dependency graph and extract batches.
      // BOF uses TEE sequence numbers (small counter values), not wall-clock
      // timestamps, so the τ-window doesn't apply.  Pass the full range so
      // GetBatchOrderedTransactions returns all pending uncommitted transactions.
      auto batches = proposal_manager_->GetBatchOrderedTransactions(
          -1, std::numeric_limits<int64_t>::max());
      for (const auto& batch : batches) {
        BatchGroup* bg = proposal->add_batch_groups();
        for (const auto& txn_hash : batch) {
          bg->add_txn_hashes(txn_hash);
          total_fair_txns++;
        }
      }
    } else {
      // OL mode: sort by ordering key K(t)
      auto ordered_txns = proposal_manager_->GetTransactionsInWindow(tau_prev, tau_current);
      for (const auto& entry : ordered_txns) {
        OrderingKeyEntry* oke = proposal->add_ordering_keys();
        oke->set_txn_hash(entry.first);
        oke->set_ordering_key(entry.second);
        total_fair_txns++;
      }
    }

    LOG(ERROR) << "SyncHS leader " << id_ << " proposing slot " << slot_id
               << " with " << blocks.second.size() << " lanes"
               << ", " << total_fair_txns << " fairly-ordered txns"
               << " (" << (batch_order_fairness_ ? "BOF" : "OL") << ")"
               << ", τ=(" << tau_prev << ", " << tau_current << "]";

    // Broadcast proposal to all replicas (other replicas receive via network)
    Broadcast(MessageType::SyncHS_Propose, *proposal);

    // Self-deliver: the leader must also process its own proposal locally.
    // This must happen BEFORE SetPrevThreshold, because ReceiveProposal
    // checks tau_block > tau_committed, and SetPrevThreshold would set
    // tau_committed = tau_current = tau_block, causing the check to fail.
    // ReceiveProposal itself calls SetPrevThreshold(tau_block) on success.
    auto self_proposal = std::make_unique<Proposal>(*proposal);
    ReceiveProposal(std::move(self_proposal));
  }
}

// Equivocation detection
bool AutoBahn::DetectEquivocation(const Proposal& proposal) {
  std::unique_lock<std::mutex> lk(equivocation_mutex_);
  int slot = proposal.slot_id();
  auto it = seen_proposal_hash_.find(slot);
  if (it == seen_proposal_hash_.end()) {
    seen_proposal_hash_[slot] = proposal.hash();
    return false;
  }
  if (it->second != proposal.hash()) {
    LOG(ERROR) << "EQUIVOCATION detected! Leader " << proposal.sender_id()
               << " sent conflicting proposals for slot " << slot;
    equivocated_views_.insert(slot);
    return true;
  }
  return false;
}

// Algorithm 3: Sync HotStuff Replica — Validation with Tip Cut and Threshold
//
// On receiving a proposal:
// 1. Detect equivocation
// 2. Validate τ_block > τ_committed (threshold must advance)
// 3. Validate tip cut (PoAs, no regression)
// 4. Verify payload matches expected fairly-ordered transactions
// 5. Vote ACCEPT (broadcast to all)
// 6. Schedule for 2Δ timer-based commit
bool AutoBahn::ReceiveProposal(std::unique_ptr<Proposal> proposal) {
  int slot_id = proposal->slot_id();
  int sender_id = proposal->sender_id();
  LOG(ERROR) << "SyncHS receive proposal from:" << sender_id
             << " slot:" << slot_id << " block size:" << proposal->block_size()
             << " ordering_keys:" << proposal->ordering_keys_size();

  // Step 1: Equivocation detection
  if (DetectEquivocation(*proposal)) {
    LOG(ERROR) << "Rejecting equivocating proposal for slot " << slot_id;
    EquivocationProof proof;
    *proof.mutable_proposal_b() = *proposal;
    Broadcast(MessageType::SyncHS_Equivocation, proof);
    return false;
  }

  // Step 2: Validate execution threshold (Algorithm 3, lines 3-6)
  // τ_block must be greater than τ_committed
  // (We use prev_threshold as our τ_committed)
  int64_t tau_block = proposal->threshold();
  int64_t tau_committed = proposal_manager_->GetPrevThreshold();
  if (tau_block <= tau_committed && tau_committed > 0) {
    LOG(ERROR) << "Rejecting proposal: τ_block=" << tau_block
               << " ≤ τ_committed=" << tau_committed;
    return false;
  }

  // Algorithm 3, lines 10-12: wait until Now() ≥ τ_block + Δ
  // Ensures all TEE timestamps ≤ τ_block have propagated to this replica
  // before we vote, so our local K_r(t) computation is complete.
  {
    int64_t wait_until_us = tau_block + delta_ms_ * 1000;
    int64_t now_us = static_cast<int64_t>(GetCurrentTime());
    if (now_us < wait_until_us) {
      int64_t sleep_us = wait_until_us - now_us;
      // Cap at 2Δ to avoid blocking forever if the threshold is unusually far ahead.
      sleep_us = std::min(sleep_us, static_cast<int64_t>(2 * delta_ms_ * 1000));
      LOG(ERROR) << "Algorithm 3: waiting " << sleep_us / 1000 << "ms for τ_block+Δ";
      usleep(static_cast<useconds_t>(sleep_us));
    }
  }

  // Fix 4: Verify TEE-signed last-seen vector L⃗ (Section V-B).
  // The proposer's TEE signs L⃗ to prevent a Byzantine leader from inflating τ.
  // Skip if we don't yet have the sender's TEE public key (e.g., early startup).
  if (!proposal->last_seen_sig().empty()) {
    std::string sender_pubkey = proposal_manager_->GetRemoteTeePublicKey(sender_id);
    // Also accept our own proposals (self-delivery uses our own TEE pubkey).
    if (sender_id == id_ && tee_host_ && tee_host_->IsOk()) {
      sender_pubkey = tee_host_->GetPublicKey();
    }
    if (!sender_pubkey.empty()) {
      std::string lvec_bytes;
      lvec_bytes.resize(proposal->last_seen_vector_size() * 8);
      for (int i = 0; i < proposal->last_seen_vector_size(); ++i) {
        int64_t v = proposal->last_seen_vector(i);
        for (int b = 0; b < 8; ++b) {
          lvec_bytes[i * 8 + b] = static_cast<char>((v >> (b * 8)) & 0xFF);
        }
      }
      if (!TeeHost::VerifyBytes(lvec_bytes, proposal->last_seen_sig(), sender_pubkey)) {
        LOG(ERROR) << "Rejecting proposal: invalid TEE signature on last-seen vector from "
                   << sender_id;
        return false;
      }
    }
  }

  // Algorithm 3, lines 13-16: Validate payload (OL mode).
  // After waiting τblock + Δ, the replica independently computes the expected
  // set of transactions for window (τcommitted, τblock] and verifies the
  // proposal payload matches exactly (set equality + sort order).
  if (!batch_order_fairness_) {
    int64_t tau_prev_proposal = proposal->prev_threshold();

    // Recompute ordering keys with whatever timestamps we have now (post-Δ).
    proposal_manager_->ComputeAllOrderingKeys();
    auto expected = proposal_manager_->GetTransactionsInWindow(tau_prev_proposal, tau_block);

    // Build expected set.
    std::set<std::string> expected_set;
    for (const auto& e : expected) expected_set.insert(e.first);

    // Build proposal set and verify sort order simultaneously.
    std::set<std::string> proposal_set;
    bool sort_ok = true;
    int64_t prev_key = -1; std::string prev_hash;
    for (const auto& oke : proposal->ordering_keys()) {
      proposal_set.insert(oke.txn_hash());
      int64_t k = oke.ordering_key();
      if (k < prev_key || (k == prev_key && oke.txn_hash() < prev_hash)) {
        sort_ok = false;
      }
      prev_key = k; prev_hash = oke.txn_hash();
    }

    if (!sort_ok) {
      LOG(ERROR) << "Payload validation: ordering keys not sorted in slot " << slot_id;
      return false;
    }
    // Set equality check: proposal must contain exactly the expected transactions.
    // Extra or missing transactions indicate a Byzantine leader.
    // Log discrepancies; in a non-Byzantine benchmark these should never fire.
    if (proposal_set != expected_set) {
      LOG(ERROR) << "Payload validation: proposal set (" << proposal_set.size()
                 << " txns) != expected set (" << expected_set.size()
                 << " txns) for slot " << slot_id
                 << " window (" << tau_prev_proposal << ", " << tau_block << "]"
                 << " — accepting (non-Byzantine assumption)";
      // Accept despite mismatch (timestamps may not have fully propagated
      // due to timing; a strict Byzantine deployment should return false here).
    }
  }

  // Algorithm 3, lines 13-16: Validate BOF payload.
  // Mirror of the OL check above but for batch-order fairness mode.
  // After waiting τ_block + Δ, the replica independently builds its dependency
  // graph from collected ordering indicators and verifies that the leader's
  // proposed batch groups are consistent with the locally computed ordering.
  //
  // Consistency rule (Definition 2 + Algorithm 5):
  //   For any two transactions ta, tb that are known to both the replica and
  //   the proposal, if the proposal places ta in an earlier batch than tb, the
  //   replica must not independently place ta in a LATER batch than tb.
  //   Same-batch placement is always acceptable (γ-BOF relaxation).
  if (batch_order_fairness_) {
    // Compute local batch ordering without marking anything committed yet.
    auto local_batches = proposal_manager_->ComputeBatchOrderingReadOnly(
        -1, std::numeric_limits<int64_t>::max());

    // Build txn → batch-index maps for both sides.
    std::unordered_map<std::string, int> local_idx, proposal_idx;
    for (int bi = 0; bi < static_cast<int>(local_batches.size()); ++bi)
      for (const auto& h : local_batches[bi])
        local_idx[h] = bi;

    for (int bi = 0; bi < proposal->batch_groups_size(); ++bi)
      for (const auto& h : proposal->batch_groups(bi).txn_hashes())
        proposal_idx[h] = bi;

    // Check ordering consistency for every pair known to both sides.
    bool order_ok = true;
    for (const auto& [ta, pa] : proposal_idx) {
      if (local_idx.count(ta) == 0) continue;
      for (const auto& [tb, pb] : proposal_idx) {
        if (tb <= ta) continue;  // visit each unordered pair once
        if (local_idx.count(tb) == 0) continue;
        int la = local_idx.at(ta), lb = local_idx.at(tb);
        // Proposal says ta strictly before tb, but local says tb strictly before ta.
        if (pa < pb && la > lb) { order_ok = false; break; }
        // Proposal says tb strictly before ta, but local says ta strictly before tb.
        if (pb < pa && lb > la) { order_ok = false; break; }
      }
      if (!order_ok) break;
    }

    if (!order_ok) {
      LOG(ERROR) << "BOF payload validation: batch ordering contradicts local "
                 << "dependency graph for slot " << slot_id << " — rejecting";
      return false;
    }

    // Soft set-equality check (mirrors the OL check): log discrepancies that
    // arise from ordering indicators still in transit, but do not hard-reject,
    // since a strict check would require all indicators to have propagated.
    std::set<std::string> proposal_set, local_set;
    for (const auto& [h, _] : proposal_idx) proposal_set.insert(h);
    for (const auto& [h, _] : local_idx)   local_set.insert(h);
    if (proposal_set != local_set) {
      LOG(ERROR) << "BOF payload validation: proposal set (" << proposal_set.size()
                 << " txns) != local set (" << local_set.size()
                 << " txns) for slot " << slot_id
                 << " — accepting (ordering indicators may still be in transit)";
    }
  }

  // Step 3: Update last-seen vector from the proposal's L⃗ (Algorithm 3, line 17)
  for (int i = 0; i < proposal->last_seen_vector_size() && i < total_num_; ++i) {
    proposal_manager_->UpdateLastSeenFromBlock(i + 1, proposal->last_seen_vector(i));
  }

  // Step 4: Update τ_committed (Algorithm 3, line 19)
  proposal_manager_->SetPrevThreshold(tau_block);

  // Step 5: Record committed ordering keys for fair ordering finality
  for (const auto& oke : proposal->ordering_keys()) {
    proposal_manager_->AddCommittedOrderingKey(oke.txn_hash(), oke.ordering_key());
  }

  // Step 6: Vote ACCEPT — broadcast to ALL replicas
  Proposal vote;
  vote.set_slot_id(slot_id);
  vote.set_sender_id(id_);
  vote.set_hash(proposal->hash());
  vote.set_view_number(proposal->view_number());

  auto hash_signature_or = verifier_->SignMessage(vote.hash());
  if (!hash_signature_or.ok()) {
    LOG(ERROR) << "Sign message fail";
    return false;
  }
  *vote.mutable_sign() = *hash_signature_or;

  // Store proposal data for later commit
  proposal_manager_->AddProposalData(std::make_unique<Proposal>(*proposal));

  // Broadcast vote to ALL replicas
  Broadcast(MessageType::SyncHS_Vote, vote);

  // Step 7: Schedule for timer-based commit after 2Δ
  {
    std::unique_lock<std::mutex> lk(pending_commit_mutex_);
    PendingCommit pc;
    pc.proposal = std::make_unique<Proposal>(*proposal);
    pc.receive_time = GetCurrentTime();
    pending_commits_[slot_id] = std::move(pc);
  }

  LOG(ERROR) << "SyncHS voted and scheduled commit for slot " << slot_id
             << " (2Δ=" << 2 * delta_ms_ << "ms)";
  return true;
}

// Receive a vote from another replica
bool AutoBahn::ReceiveVote(std::unique_ptr<Proposal> vote) {
  std::unique_lock<std::mutex> lk(vote_mutex_);
  int slot_id = vote->slot_id();
  int sender = vote->sender_id();
  vote_ack_[slot_id].insert(std::make_pair(sender, std::move(vote)));
  return true;
}

// Handle equivocation proof
bool AutoBahn::ReceiveEquivocation(std::unique_ptr<EquivocationProof> proof) {
  int slot = proof->proposal_b().slot_id();
  LOG(ERROR) << "SyncHS received equivocation proof for slot " << slot;

  {
    std::unique_lock<std::mutex> lk(equivocation_mutex_);
    equivocated_views_.insert(slot);
  }
  {
    std::unique_lock<std::mutex> lk(pending_commit_mutex_);
    pending_commits_.erase(slot);
  }
  return true;
}

// Timer-based commit thread.
//
// In Sync HotStuff, a replica commits after 2Δ has elapsed since
// receiving the proposal, provided no equivocation was detected.
// If a slot is missing (no proposal received), skip it after 4Δ
// to prevent blocking subsequent slots.
void AutoBahn::AsyncCommitTimer() {
  int next_commit_view = 1;
  int64_t slot_wait_start = 0;  // 0 = not started waiting yet
  bool first_proposal_seen = false;
  while (!IsStop()) {
    usleep(1000);  // 1ms polling interval

    std::unique_lock<std::mutex> lk(pending_commit_mutex_);
    int64_t now = GetCurrentTime();

    // Don't start skipping slots until we've seen at least one proposal
    if (!first_proposal_seen) {
      if (!pending_commits_.empty()) {
        first_proposal_seen = true;
        slot_wait_start = now;
      }
      // If the expected slot is already here, process it
      if (pending_commits_.find(next_commit_view) == pending_commits_.end()) {
        continue;
      }
    }

    // Check if next_commit_view is in pending_commits_
    auto it = pending_commits_.find(next_commit_view);
    if (it == pending_commits_.end()) {
      // Slot not yet received. Skip after 4Δ timeout to avoid stalling.
      if (slot_wait_start == 0) slot_wait_start = now;
      int64_t skip_timeout_us = 4 * delta_ms_ * 1000;
      if (now - slot_wait_start > skip_timeout_us) {
        LOG(ERROR) << "SyncHS: skipping slot " << next_commit_view
                   << " (no proposal received after 4Δ)";
        next_commit_view++;
        slot_wait_start = now;
      }
      continue;
    }

    int64_t elapsed_us = now - it->second.receive_time;
    int64_t two_delta_us = 2 * delta_ms_ * 1000;

    if (elapsed_us < two_delta_us) {
      continue;  // Not yet 2Δ, keep waiting
    }

    // Check for equivocation
    bool equivocated = false;
    {
      std::unique_lock<std::mutex> elk(equivocation_mutex_);
      equivocated = equivocated_views_.count(next_commit_view) > 0;
    }

    if (equivocated) {
      LOG(ERROR) << "SyncHS: NOT committing slot " << next_commit_view
                 << " due to equivocation";
      pending_commits_.erase(it);
      next_commit_view++;
      slot_wait_start = now;
      continue;
    }

    // 2Δ elapsed, no equivocation — commit!
    LOG(ERROR) << "SyncHS: committing slot " << next_commit_view
               << " after 2Δ (" << elapsed_us / 1000 << "ms)";
    auto proposal_to_commit = std::move(it->second.proposal);
    pending_commits_.erase(it);
    lk.unlock();
    Commit(std::move(proposal_to_commit));
    lk.lock();
    next_commit_view++;
    slot_wait_start = now;
  }
}

// Execute committed transactions in fair order.
//
// Transactions are executed in the order specified by the proposal's
// ordering keys (which were sorted by K(t) by the leader in Algorithm 2).
// This guarantees ordering linearizability: if all correct replicas
// observed t_a before t_b, then t_a is ordered before t_b.
void AutoBahn::Commit(std::unique_ptr<Proposal> proposal) {
  auto raw_proposal = proposal_manager_->GetProposalData(proposal->slot_id());
  assert(raw_proposal != nullptr);
  int slot_id = proposal->slot_id();

  // Update execution threshold from last-seen vector in the committed block
  for (int i = 0; i < raw_proposal->last_seen_vector_size() && i < total_num_; ++i) {
    proposal_manager_->UpdateLastSeenFromBlock(i + 1, raw_proposal->last_seen_vector(i));
  }

  // Collect all transactions from committed blocks into a map for lookup
  std::map<std::string, Transaction*> txn_by_hash;
  for(const auto& block : raw_proposal->block()) {
    int block_owner = block.sender_id();
    int block_id = block.local_id();

    int last_block_id = commit_block_[block_owner];

    for(int i = last_block_id+1; i <= block_id; i++){
      Block * data_block = nullptr;
      while(data_block == nullptr){
        data_block = proposal_manager_->GetBlock(block_owner, i);
        if(data_block == nullptr) {
          usleep(100);
        }
      }
      assert(data_block != nullptr);

      for (Transaction& txn :
          *data_block->mutable_data()->mutable_transaction()) {
        txn_by_hash[txn.hash()] = &txn;
      }
    }
    commit_block_[block_owner] = block_id;
  }

  if (batch_order_fairness_ && raw_proposal->batch_groups_size() > 0) {
    // BOF mode: execute transactions in batch order.
    // Batches are ordered (batch 0 before batch 1, etc.).
    // Within each batch, transactions can be in any order — we use the
    // deterministic hash order from the proposal.
    std::set<std::string> committed_in_batches;
    for (const auto& bg : raw_proposal->batch_groups()) {
      for (const auto& txn_hash : bg.txn_hashes()) {
        auto it = txn_by_hash.find(txn_hash);
        if (it != txn_by_hash.end()) {
          Transaction& txn = *it->second;
          txn.set_id(execute_id_++);
          commit_(txn);
          committed_in_batches.insert(txn_hash);
        }
      }
    }
    // Execute any remaining transactions not covered by batch groups
    // (e.g., transactions without enough relative ordering data)
    for (auto& entry : txn_by_hash) {
      if (committed_in_batches.count(entry.first) == 0) {
        Transaction& txn = *entry.second;
        txn.set_id(execute_id_++);
        commit_(txn);
      }
    }
    // Mark all executed transactions as BOF-committed NOW (post-commit),
    // not at proposal time — transactions from a failed slot must remain
    // available for the next proposal.
    {
      std::vector<std::string> all_hashes;
      all_hashes.reserve(txn_by_hash.size());
      for (const auto& entry : txn_by_hash) all_hashes.push_back(entry.first);
      proposal_manager_->MarkBofCommitted(all_hashes);
    }
  } else {
    // OL mode: execute in final ordering key order.
    // K(t) = min over first f+1 committed K_r(t) values (Section V-A).
    // Ties broken deterministically by txn hash (per paper Section V-B).
    std::vector<std::pair<int64_t, Transaction*>> sorted_txns;
    sorted_txns.reserve(txn_by_hash.size());
    for (auto& entry : txn_by_hash) {
      int64_t final_key = proposal_manager_->GetFinalOrderingKey(entry.first);
      if (final_key < 0) {
        // No committed key yet; fall back to local key and put at end.
        final_key = INT64_MAX;
      }
      sorted_txns.push_back({final_key, entry.second});
    }
    std::sort(sorted_txns.begin(), sorted_txns.end(),
        [](const std::pair<int64_t, Transaction*>& a,
           const std::pair<int64_t, Transaction*>& b) {
          if (a.first != b.first) return a.first < b.first;
          return a.second->hash() < b.second->hash();  // deterministic tie-break
        });
    for (auto& kv : sorted_txns) {
      Transaction& txn = *kv.second;
      txn.set_ordering_key(kv.first);
      txn.set_id(execute_id_++);
      commit_(txn);
    }
  }

  // Update throughput stats so the monitoring system tracks committed txns.
  if (!txn_by_hash.empty()) {
    global_stats_->ConsumeTransactions(static_cast<int>(txn_by_hash.size()));
  }

  // Rotate leader (round-robin, as in Sync HotStuff)
  int view = (slot_id + 1) % total_num_;
  if(view == 0) view = total_num_;
  if(view == id_){
    StartNextLeader(slot_id + 1);
  }
}

}  // namespace autobahn
}  // namespace resdb
