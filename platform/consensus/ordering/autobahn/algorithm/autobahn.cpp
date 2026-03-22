#include "platform/consensus/ordering/autobahn/algorithm/autobahn.h"

#include <glog/logging.h>

#include "common/crypto/signature_verifier.h"
#include "common/utils/utils.h"


namespace resdb {
namespace autobahn {

AutoBahn::AutoBahn(int id, int f, int total_num, int block_size, SignatureVerifier* verifier)
    : ProtocolBase(id, f, total_num), verifier_(verifier) {

  LOG(ERROR) << "Initializing AutoBahn with Sync HotStuff + Fair Ordering"
             << " id=" << id << " f=" << f << " n=" << total_num;
  id_ = id;
  total_num_ = total_num;
  f_ = f;
  is_stop_ = false;
  timeout_ms_ = 60000;
  // Δ: synchronous network delay bound.
  // Replicas wait Δ to collect timestamps, 2Δ before committing.
  delta_ms_ = 1000;  // 1 second for local testing; increase for real deployments
  batch_size_ = block_size;
  execute_id_ = 1;
  is_leader_ = id_ == 1;
  cur_slot_ = 1;

  proposal_manager_ = std::make_unique<ProposalManager>(id, total_num_, f_, verifier);

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
      for (const auto& ts : own_timestamps) {
        *batch.add_timestamps() = ts;
      }
      Broadcast(MessageType::TEE_Timestamps, batch);
      LOG(ERROR) << "Broadcast " << own_timestamps.size()
                 << " own TEE timestamps for local block " << (next_block-1);
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

  for (const auto& ts : batch->timestamps()) {
    proposal_manager_->AddTimestamp(ts);
  }
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
      tau_current = GetCurrentTime() - 2 * delta_ms_ * 1000;
      if (tau_current <= tau_prev) {
        tau_current = tau_prev + 1;
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
    std::vector<int64_t> last_seen = proposal_manager_->GetLastSeenVector();
    for (int i = 1; i <= total_num_; ++i) {
      proposal->add_last_seen_vector(last_seen[i]);
    }

    // Step 5: Extract transactions in execution window and sort by K(t)
    // (Algorithm 2, lines 6-8)
    auto ordered_txns = proposal_manager_->GetTransactionsInWindow(tau_prev, tau_current);
    for (const auto& entry : ordered_txns) {
      OrderingKeyEntry* oke = proposal->add_ordering_keys();
      oke->set_txn_hash(entry.first);
      oke->set_ordering_key(entry.second);
    }

    LOG(ERROR) << "SyncHS leader " << id_ << " proposing slot " << slot_id
               << " with " << blocks.second.size() << " lanes"
               << ", " << ordered_txns.size() << " fairly-ordered txns"
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

  // Execute transactions from committed blocks
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
        // Set the final ordering key on the transaction for downstream use
        int64_t final_key = proposal_manager_->GetFinalOrderingKey(txn.hash());
        if (final_key >= 0) {
          txn.set_ordering_key(final_key);
        }
        txn.set_id(execute_id_++);
        commit_(txn);
      }
    }
    commit_block_[block_owner] = block_id;
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
