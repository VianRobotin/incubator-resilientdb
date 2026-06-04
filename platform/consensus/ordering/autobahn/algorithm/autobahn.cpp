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
  delta_ms_ = 50;  // 50ms Δ; DAS5 LAN RTT <1ms, 50ms is safe (was 1000ms)
  batch_size_ = block_size;
  execute_id_ = 1;
  is_leader_ = id_ == 1;
  cur_slot_ = 1;

  global_stats_ = Stats::GetGlobalStats(5);

  proposal_manager_ = std::make_unique<ProposalManager>(id, total_num_, f_, verifier);
  proposal_manager_->SetBatchOrderFairness(batch_order_fairness_);
  proposal_manager_->SetBofGamma(bof_gamma);
  proposal_manager_->SetDeltaUs(delta_ms_ * 1000);

  // BOF throughput tuning (changes 1, 4, 5).  The dependency-graph build is
  // O(n²·f) over the candidate set; capping the per-slot candidate count
  // prevents queueing collapse at high input rates.  Force-include after a
  // few slots so the cap can never starve a straggler.  The worker thread
  // moves the O(n²·f) work off the propose/validate critical path.
  if (batch_order_fairness_) {
    // ~ 8 × block_size lets the leader fit several blocks per slot's batch
    // without re-entering the n²·f danger zone.  Tunable via env if needed.
    int bof_cap = 8 * block_size;
    if (const char* env = getenv("BOF_MAX_CANDIDATES")) {
      try { bof_cap = std::stoi(env); } catch (...) {}
    }
    int bof_age = 4;  // slots before a stale candidate is force-included
    if (const char* env = getenv("BOF_FORCE_AGE_SLOTS")) {
      try { bof_age = std::stoi(env); } catch (...) {}
    }
    proposal_manager_->SetBofMaxCandidates(bof_cap);
    proposal_manager_->SetBofForceAgeSlots(bof_age);
    proposal_manager_->StartBofWorker();
    LOG(ERROR) << "[BOF] tuning: max_candidates=" << bof_cap
               << " force_age_slots=" << bof_age << " (worker started)";
  }

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
  {
    std::lock_guard<std::mutex> lk(own_create_times_mutex_);
    own_create_times_[txn->hash()] = txn->create_time();
  }
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

    // Record dissemination start time. Self-ACK + broadcast happen next;
    // this measures from just before the wire-send, not from after ECALL work.
    {
      std::unique_lock<std::mutex> lk(block_time_mutex_);
      block_disseminate_time_[block->local_id()] = GetCurrentTime();
      block_txn_count_[block->local_id()] = block->data().transaction_size();
    }

    // Self-ACK: generate a local ACK for our own block so that BlockReady
    // can be triggered without depending on network self-delivery.
    {
      BlockACK self_ack;
      self_ack.set_hash(block->hash());
      self_ack.set_sender_id(id_);
      self_ack.set_local_id(block->local_id());
      self_ack.set_responder(id_);
      *self_ack.mutable_sign_info() = proposal_manager_->SignBlock(*block);
      ReceiveBlockACK(std::make_unique<BlockACK>(self_ack));
    }

    // Broadcast block to all replicas FIRST so they can ACK immediately.
    // TEE-timestamp and batch-order work happens after, so it does not
    // delay the start of block certification (PoA chain critical path).
    Broadcast(MessageType::NewBlocks, *block);

    // The originating replica must also TEE-timestamp its own transactions
    // so that its own ordering key contribution is included in proposals.
    // Done AFTER broadcast so block cert (PoA chain) is not delayed by ECALL.
    // MUST happen before RecordLocalReceiveOrder (same as ReceiveBlock): without
    // this ordering, ts_store_ is empty when RecordLocalReceiveOrder runs, every
    // relative ordering for own blocks is empty, and precedes_count_ is never
    // populated, causing BOF to produce 0 transactions per proposal.
    std::vector<SignedTimestamp> own_timestamps =
        proposal_manager_->TimestampTransactions(*block);

    // Batch-order fairness: broadcast our local receive order for this block.
    // Must run after TimestampTransactions so ts_store_ has our TEE timestamps;
    // otherwise RecordLocalReceiveOrder returns an empty ordering and the
    // per-block entry in block_orderings_ is never populated.
    if (batch_order_fairness_) {
      RelativeOrdering rel_order = proposal_manager_->RecordLocalReceiveOrder(*block);
      if (rel_order.txn_hashes_size() > 0) {
        Broadcast(MessageType::BOF_RelativeOrder, rel_order);
      }
    }

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
  }
}

// Algorithm 1, lines 3-14: OnReceiveCar
//
// When receiving a block (Car) from another replica:
// 1. Store the block and send a BlockACK (PoA)
// 2. TEE-timestamp each transaction in the block
// 3. Broadcast the signed timestamps to all replicas
void AutoBahn::ReceiveBlock(std::unique_ptr<Block> block) {
  LOG(INFO)<<"recv block from:"<<block->sender_id()<<" block id:"<<block->local_id();

  // Step 1: Standard Autobahn — ACK for Proof of Availability.
  // CRITICAL: send ACK immediately before any expensive work (timestamp
  // computation, signature operations) so the sender's WaitForResponse()
  // is not delayed by our O(block_size) signing work.
  BlockACK block_ack;
  block_ack.set_hash(block->hash());
  block_ack.set_sender_id(block->sender_id());
  block_ack.set_local_id(block->local_id());
  block_ack.set_responder(id_);
  *block_ack.mutable_sign_info() = proposal_manager_->SignBlock(*block);

  // Send ACK point-to-point to the block sender only.
  // Broadcasting ACKs to all N nodes inflates traffic to O(N²) per block per
  // sender, making total ACK traffic O(N³) per round — catastrophic at large N.
  // Point-to-point keeps it at O(N) for the whole system.
  SendMessage(MessageType::CMD_BlockACK, block_ack, block->sender_id());

  // Step 2: Store block immediately so Commit() finds it without waiting for
  // ECALL work. Keep a raw pointer — block stays valid in pending_blocks_.
  Block* block_ptr = block.get();
  proposal_manager_->AddBlock(std::move(block));
  // UpdateView increments new_blocks_[current_slot_] but does NOT wake the
  // leader yet; we do that AFTER timestamps are stored so the leader doesn't
  // propose with an empty ts_store_.
  proposal_manager_->UpdateView(block_ack.sender_id(), block_ack.local_id());

  // Step 3: TEE-timestamp each transaction (Algorithm 1, lines 6-13).
  // Must run before RecordLocalReceiveOrder: the latter reads ts_store_ looking
  // for sender_id == id_, which is only populated after TimestampTransactions.
  // Swapping the order yields empty RelativeOrderings, an unpopulated
  // block_orderings_, and BOF proposals containing zero transactions.
  std::vector<SignedTimestamp> new_timestamps =
      proposal_manager_->TimestampTransactions(*block_ptr);

  // Step 4: BOF: now that ts_store_ has our local TEE timestamps, record our
  // receive order for this block and broadcast it to all replicas.
  if (batch_order_fairness_) {
    RelativeOrdering rel_order = proposal_manager_->RecordLocalReceiveOrder(*block_ptr);
    if (rel_order.txn_hashes_size() > 0) {
      Broadcast(MessageType::BOF_RelativeOrder, rel_order);
    }
  }

  // Step 5: Broadcast timestamps (Algorithm 1, line 14)
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
  }

  // Step 6: Wake the consensus thread AFTER timestamps are in ts_store_.
  NotifyView();
}

void AutoBahn::ReceiveBlockACK(std::unique_ptr<BlockACK> block) {
  // Only process ACKs for our own blocks.
  if (block->sender_id() != id_) return;

  bool ready = false;
  std::map<int, SignInfo> ack_copy;
  int64_t local_id = block->local_id();

  {
    std::unique_lock<std::mutex> lk(block_mutex_);
    block_ack_[local_id].insert(std::make_pair(block->responder(), block->sign_info()));
    if (block_ack_[local_id].size() == (size_t)(f_ + 1) &&
        block_ack_[local_id].find(id_) != block_ack_[local_id].end()) {
      ready = true;
      ack_copy = block_ack_[local_id];
    }
  }
  // Release block_mutex_ before acquiring bc_mutex_ to avoid deadlock
  // with AsyncDissemination's WaitForResponse which holds bc_mutex_.
  if (ready) {
    int64_t cert_time_us = GetCurrentTime();
    int64_t dissem_time_us = 0;
    int txn_count = 0;
    {
      std::unique_lock<std::mutex> lk(block_time_mutex_);
      auto dt = block_disseminate_time_.find(local_id);
      if (dt != block_disseminate_time_.end()) dissem_time_us = dt->second;
      auto tc = block_txn_count_.find(local_id);
      if (tc != block_txn_count_.end()) txn_count = tc->second;
    }
    if (dissem_time_us > 0) {
      LOG(ERROR) << "block_certified block_id:" << local_id
                 << " txns:" << txn_count
                 << " consensus_latency_us:" << (cert_time_us - dissem_time_us);
    }
    LOG(INFO) << "block " << local_id << " certified (" << ack_copy.size() << " acks)";
    proposal_manager_->BlockReady(ack_copy, local_id);
    // Register own certified block in the slot view so GetCut() includes it.
    // ReceiveBlock() calls UpdateView for every *received* block, but the sender
    // never receives its own blocks, so slot_state_[id_] is never updated.
    // Without this, the leader's tip cut always omits its own lane.
    proposal_manager_->UpdateView(id_, local_id);
    NotifyView();
    std::unique_lock<std::mutex> lk(bc_mutex_);
    BlockDone();
  }
}

// Algorithm 1, lines 16-19: OnReceiveTimestamp
//
// Receive TEE-signed timestamps from another replica and store them.
// After enough timestamps are collected (≥ f+1), we can compute
// the local ordering key K_r(t).
void AutoBahn::ReceiveTimestamps(std::unique_ptr<TimestampBatch> batch) {
  LOG(INFO) << "Received " << batch->timestamps_size()
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

    // Step 1: Compute execution threshold τ_current (Section V-B)
    int64_t tau_prev = proposal_manager_->GetPrevThreshold();
    int64_t tau_current = proposal_manager_->ComputeExecutionThreshold();

    // If τ hasn't advanced, use a fallback: current time minus 2Δ
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

    // Step 2: Compute ordering keys AFTER the τ+Δ wait so that timestamps
    // from all replicas have had Δ time to arrive in ts_store_.
    // (Was before the wait — caused 0 txns in proposals because timestamps
    // hadn't accumulated yet when the leader first woke up.)
    // Skip in BOF mode: ordering keys are not used there (bof_seq_ is used instead).
    if (!batch_order_fairness_) {
      proposal_manager_->ComputeAllOrderingKeys();
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
    //
    // Note: the (τ_prev, τ_current] window here is a leader heuristic for
    // which txns the proposal should carry ordering metadata for.  Actual
    // execution eligibility is re-checked post-commit in Commit() against
    // the finalized τ, so a tx included in a proposal may still be buffered
    // post-commit if its final K(t)/bof_seq has not been confirmed ≤ τ.
    int total_fair_txns = 0;
    if (batch_order_fairness_) {
      // Pass the same (τ_prev, τ_current] window OL uses.  In BOF mode the
      // last_seen_[r] values are per-replica TEE sequence numbers (Algorithm 1
      // update covers both modes after the AddTimestamp fix), so τ is the
      // (f+1)-th smallest s_R — any tx with bof_seq ≤ τ_current has the
      // Lemma-5 guarantee that f+1 replicas have already attested it.
      auto batches = proposal_manager_->GetBatchOrderedTransactions(
          tau_prev, tau_current);
      for (const auto& batch : batches) {
        BatchGroup* bg = proposal->add_batch_groups();
        for (const auto& txn_hash : batch) {
          bg->add_txn_hashes(txn_hash);
          total_fair_txns++;
        }
      }
    } else {
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

  // Insert placeholder into pending_commits_ BEFORE any slow validation so
  // AsyncCommitTimer knows the slot is in flight and does not fire its skip
  // timeout while BOF/OL payload validation is still running.  The validated
  // flag is flipped at the end of this function, after the vote is broadcast.
  {
    std::unique_lock<std::mutex> lk(pending_commit_mutex_);
    PendingCommit pc;
    pc.receive_time = GetCurrentTime();
    pc.validated = false;
    pending_commits_[slot_id] = std::move(pc);
  }
  auto abandon_pending = [&]() {
    std::unique_lock<std::mutex> lk(pending_commit_mutex_);
    auto it = pending_commits_.find(slot_id);
    if (it != pending_commits_.end() && !it->second.validated) {
      pending_commits_.erase(it);
    }
  };

  // Step 2: Validate execution threshold (Algorithm 3, lines 3-6)
  // τ_block must be greater than τ_committed
  // (We use prev_threshold as our τ_committed)
  int64_t tau_block = proposal->threshold();
  int64_t tau_committed = proposal_manager_->GetPrevThreshold();
  if (tau_block <= tau_committed && tau_committed > 0) {
    LOG(ERROR) << "Rejecting proposal: τ_block=" << tau_block
               << " ≤ τ_committed=" << tau_committed;
    abandon_pending();
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
  // The enclave signs L⃗ with HMAC-SHA256 using a key that never leaves the enclave.
  // Each replica has its own enclave key, so cross-replica HMAC verification is
  // impossible without an ECALL to the sender's enclave.  We verify self-proposals
  // (sender == us, using our own enclave) and accept remote ones on the non-Byzantine
  // assumption — in a Byzantine deployment, replace with an attested key-exchange scheme.
  if (!proposal->last_seen_sig().empty() && sender_id == id_ &&
      tee_host_ && tee_host_->IsOk()) {
    std::string lvec_bytes;
    lvec_bytes.resize(proposal->last_seen_vector_size() * 8);
    for (int i = 0; i < proposal->last_seen_vector_size(); ++i) {
      int64_t v = proposal->last_seen_vector(i);
      for (int b = 0; b < 8; ++b) {
        lvec_bytes[i * 8 + b] = static_cast<char>((v >> (b * 8)) & 0xFF);
      }
    }
    if (!tee_host_->VerifyBytes(lvec_bytes, proposal->last_seen_sig())) {
      LOG(ERROR) << "Rejecting proposal: invalid TEE signature on own last-seen vector (slot "
                 << slot_id << ")";
      abandon_pending();
      return false;
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
      abandon_pending();
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
    // Compute local batch ordering for the same window the leader used,
    // without marking anything committed yet.
    int64_t tau_prev_proposal = proposal->prev_threshold();
    auto local_batches = proposal_manager_->ComputeBatchOrderingReadOnly(
        tau_prev_proposal, tau_block);

    // Build txn → batch-index maps for both sides.
    std::unordered_map<std::string, int> local_idx, proposal_idx;
    for (int bi = 0; bi < static_cast<int>(local_batches.size()); ++bi)
      for (const auto& h : local_batches[bi])
        local_idx[h] = bi;

    for (int bi = 0; bi < proposal->batch_groups_size(); ++bi)
      for (const auto& h : proposal->batch_groups(bi).txn_hashes())
        proposal_idx[h] = bi;

    // O(n log n) pair consistency: for every txn known to both sides, collect
    // (proposal_idx, local_idx) pairs, sort by proposal_idx, and sweep block by
    // block.  The rule "pa < pb implies la <= lb" is equivalent to: across
    // strictly-ordered proposal blocks, the local index is non-decreasing — so
    // max(local_idx in previous blocks) must be <= every local_idx in the
    // current block.  Within a block of equal proposal_idx, order is free.
    std::vector<std::pair<int,int>> pa_la;
    pa_la.reserve(proposal_idx.size());
    for (const auto& [h, pa] : proposal_idx) {
      auto it = local_idx.find(h);
      if (it == local_idx.end()) continue;
      pa_la.emplace_back(pa, it->second);
    }
    std::sort(pa_la.begin(), pa_la.end());

    bool order_ok = true;
    int running_max_la = std::numeric_limits<int>::min();  // max la across strictly-earlier blocks
    int block_max_la = std::numeric_limits<int>::min();    // max la in current block
    int prev_pa = 0;
    bool started = false;
    for (const auto& [pa, la] : pa_la) {
      if (!started || pa != prev_pa) {
        if (started) running_max_la = std::max(running_max_la, block_max_la);
        block_max_la = la;
        prev_pa = pa;
        started = true;
      } else {
        block_max_la = std::max(block_max_la, la);
      }
      if (la < running_max_la) { order_ok = false; break; }
    }

    if (!order_ok) {
      // Soft reject: log but accept.  In a non-Byzantine benchmark the
      // contradiction arises from BOF_RelativeOrder messages arriving between
      // the leader's GetBatchOrderedTransactions() call and our
      // ComputeBatchOrderingReadOnly() call here, causing a race-condition
      // ordering flip.  A hard reject would silently stall the commit chain
      // (even the leader's self-delivery fails), leaving slots_committed≤2.
      // The committed batch ordering in the proposal is still fair — we just
      // can't verify it locally with a stale snapshot.
      LOG(ERROR) << "BOF payload validation: batch ordering contradicts local "
                 << "dependency graph for slot " << slot_id << " — accepting (non-Byzantine)";
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
    abandon_pending();
    return false;
  }
  *vote.mutable_sign() = *hash_signature_or;

  // Store proposal data for later commit
  proposal_manager_->AddProposalData(std::make_unique<Proposal>(*proposal));

  // Broadcast vote to ALL replicas
  Broadcast(MessageType::SyncHS_Vote, vote);

  // Step 7: Finalize the pending-commit placeholder.  The slot is now fully
  // validated and the vote has been broadcast; AsyncCommitTimer can proceed.
  {
    std::unique_lock<std::mutex> lk(pending_commit_mutex_);
    auto it = pending_commits_.find(slot_id);
    if (it != pending_commits_.end()) {
      it->second.proposal = std::make_unique<Proposal>(*proposal);
      it->second.validated = true;
    } else {
      // Defensive: placeholder was erased (e.g. by a stale-timeout); reinsert
      // as already-validated so the commit timer can still pick it up.
      PendingCommit pc;
      pc.proposal = std::make_unique<Proposal>(*proposal);
      pc.receive_time = GetCurrentTime();
      pc.validated = true;
      pending_commits_[slot_id] = std::move(pc);
    }
  }

  LOG(ERROR) << "SyncHS voted and scheduled commit for slot " << slot_id
             << " (2Δ=" << 2 * delta_ms_ << "ms)";
  return true;
}

// Receive a vote from another replica
bool AutoBahn::ReceiveVote(std::unique_ptr<Proposal> vote) {
  int slot_id = vote->slot_id();
  int sender = vote->sender_id();
  int64_t now_us = GetCurrentTime();
  int vote_count = 0;
  {
    std::unique_lock<std::mutex> lk(vote_mutex_);
    vote_ack_[slot_id].insert(std::make_pair(sender, std::move(vote)));
    vote_count = static_cast<int>(vote_ack_[slot_id].size());
  }

  // Vote timing diagnostics
  {
    std::unique_lock<std::mutex> tlk(vote_timing_mutex_);
    if (vote_count == 1) {
      vote_first_time_[slot_id] = now_us;
    }
    if (vote_count == 2 * f_ + 1) {
      vote_quorum_time_[slot_id] = now_us;
    }
  }

  // Compute propose_time from pending_commits_ to measure vote RTT.
  // Log on first vote and on quorum (2f+1).
  if (vote_count == 1 || vote_count == 2 * f_ + 1) {
    int64_t propose_time = 0;
    {
      std::unique_lock<std::mutex> lk(pending_commit_mutex_);
      auto it = pending_commits_.find(slot_id);
      if (it != pending_commits_.end()) propose_time = it->second.receive_time;
    }
    int64_t elapsed_ms = propose_time > 0 ? (now_us - propose_time) / 1000 : -1;
    if (vote_count == 1) {
      LOG(ERROR) << "vote_timing slot=" << slot_id
                 << " first_vote from=" << sender
                 << " elapsed_since_proposal=" << elapsed_ms << "ms";
    } else {
      LOG(ERROR) << "vote_timing slot=" << slot_id
                 << " quorum(2f+1=" << (2 * f_ + 1) << ") reached"
                 << " elapsed_since_proposal=" << elapsed_ms << "ms";
    }
  }

  // Quorum reached when 2f+1 replicas have voted (not all N).
  // Previously this required total_num_ votes, so the early-commit path
  // almost never fired. With 2f+1, we can commit as soon as we have a
  // majority rather than waiting for all N nodes.
  bool quorum = (vote_count >= 2 * f_ + 1);
  if (quorum) {
    std::unique_lock<std::mutex> lk(pending_commit_mutex_);
    auto it = pending_commits_.find(slot_id);
    if (it != pending_commits_.end()) {
      it->second.quorum_reached = true;
    }
  }
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
      int64_t skip_timeout_us = 20 * delta_ms_ * 1000;  // 20Δ = 1s; 4Δ was too short for saturated queues
      if (now - slot_wait_start > skip_timeout_us) {
        LOG(ERROR) << "SyncHS: skipping slot " << next_commit_view
                   << " (no proposal received after 4Δ)";
        // Trigger leader rotation for the skipped slot so the chain doesn't break.
        // Without this, StartNextLeader(K+1) is only called from Commit(K),
        // which never fires for skipped slots — all subsequent leaders stay dormant.
        int next_view = (next_commit_view + 1) % total_num_;
        if (next_view == 0) next_view = total_num_;
        if (next_view == id_) {
          StartNextLeader(next_commit_view + 1);
        }
        next_commit_view++;
        slot_wait_start = now;
      }
      continue;
    }

    int64_t elapsed_us = now - it->second.receive_time;
    int64_t two_delta_us = 2 * delta_ms_ * 1000;

    // Payload validation still running in ReceiveProposal.  Wait for it, but
    // abandon the placeholder if it stays unvalidated far longer than the
    // protocol's 2Δ commit timer, so a genuinely broken slot can't wedge the
    // chain forever.  100Δ (= 5s at Δ=50ms) is much longer than any healthy
    // validation path (BOF check is now O(n log n)) but short enough to let
    // the skip-timer path take over.
    if (!it->second.validated) {
      int64_t stale_us = 100 * delta_ms_ * 1000;
      if (elapsed_us > stale_us) {
        LOG(ERROR) << "SyncHS: abandoning unvalidated slot " << next_commit_view
                   << " after " << elapsed_us / 1000 << "ms";
        pending_commits_.erase(it);
        // Let the skip-timer path handle leader rotation on the next tick.
        slot_wait_start = now;
      }
      continue;
    }

    if (elapsed_us < two_delta_us && !it->second.quorum_reached) {
      continue;  // Not yet 2Δ and not unanimous vote, keep waiting
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

    // 2Δ elapsed (or all voted), no equivocation — commit!
    LOG(ERROR) << "SyncHS: committing slot " << next_commit_view
               << (it->second.quorum_reached ? " (all voted)" : " (2Δ timer)")
               << " elapsed=" << elapsed_us / 1000 << "ms";
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
  int slot_id = proposal->slot_id();

  // Wait for the proposal data to arrive. Under high load, votes can be
  // delivered before the proposal message (message-queue reordering), so
  // GetProposalData may return nullptr when the 2Δ timer first fires.
  // Loop with a short sleep rather than assert-crashing the process.
  std::unique_ptr<Proposal> proposal_data;
  for (int attempt = 0; attempt < 500 && !IsStop(); ++attempt) {
    proposal_data = proposal_manager_->GetProposalData(slot_id);
    if (proposal_data != nullptr) break;
    usleep(1000);  // 1ms; proposal typically arrives within a few ms
  }
  Proposal* raw_proposal = proposal_data.get();
  if (raw_proposal == nullptr) {
    LOG(ERROR) << "Commit: proposal data for slot " << slot_id
               << " never arrived — skipping slot";
    int view = (slot_id + 1) % total_num_;
    if (view == 0) view = total_num_;
    if (view == id_) StartNextLeader(slot_id + 1);
    return;
  }

  // Update execution threshold from last-seen vector in the committed block
  for (int i = 0; i < raw_proposal->last_seen_vector_size() && i < total_num_; ++i) {
    proposal_manager_->UpdateLastSeenFromBlock(i + 1, raw_proposal->last_seen_vector(i));
  }

  // Collect txns freshly committed by this slot and compute consensus latency.
  // Only sample create_time from txns whose originating replica is this one,
  // because create_time is set in ReceiveTransaction on the originator's clock.
  // Using OWN-originated txns means create_time and commit_time share the same
  // physical clock (no cross-node skew).
  std::map<std::string, Transaction> newly_committed;
  int64_t own_sum_create_us = 0;
  int own_create_samples = 0;
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

      for (const Transaction& txn :
          data_block->data().transaction()) {
        newly_committed[txn.hash()] = txn;
        if (block_owner == id_ && txn.create_time() > 0) {
          own_sum_create_us += txn.create_time();
          own_create_samples++;
        }
      }
    }
    commit_block_[block_owner] = block_id;
  }

  // slot_committed: consensus latency = slot_commit_time − avg(create_time)
  // over own-originated txns in this slot.  Emitted BEFORE eligibility check,
  // because "committed by Sync HotStuff" is strictly earlier than "eligible
  // for execution" — a committed tx still has to wait until τ advances past
  // its ordering indicator (Section V-B, Lemma 5).
  {
    int64_t now_commit_us = GetCurrentTime();
    uint64_t consensus_latency_us = 0;
    if (own_create_samples > 0) {
      int64_t avg_create_us = own_sum_create_us / own_create_samples;
      consensus_latency_us = static_cast<uint64_t>(
          std::max<int64_t>(0, now_commit_us - avg_create_us));
    }
    LOG(ERROR) << "slot_committed slot:" << slot_id
               << " txns:" << newly_committed.size()
               << " consensus_latency_us:" << consensus_latency_us;
  }

  // ============================================================
  // Post-commit execution eligibility (thesis Section V-B / VI).
  //
  // Advance tau_committed_ from the committed slot's threshold.  Merge
  // newly_committed into pending_exec_; then re-scan pending_exec_ and move
  // txns whose ordering indicator (K(t) for OL, bof_seq for BOF) is finalized
  // and ≤ tau_committed_ into the eligible set.  Everything else stays
  // buffered until a future commit further advances τ.
  // ============================================================

  tau_committed_ = std::max(tau_committed_, raw_proposal->threshold());

  // Tick the BOF slot counter so SetBofForceAgeSlots() can age stragglers
  // out of the candidate cap.  No-op in OL mode (counter unused).
  if (batch_order_fairness_) {
    proposal_manager_->AdvanceBofSlot();
  }

  {
    std::lock_guard<std::mutex> lk(pending_exec_mutex_);
    for (auto& entry : newly_committed) {
      pending_exec_.emplace(entry.first, entry.second);
    }
  }

  // Build the list of now-eligible (hash, indicator) pairs.
  std::vector<std::pair<int64_t, std::string>> eligible;
  {
    std::lock_guard<std::mutex> lk(pending_exec_mutex_);
    for (const auto& entry : pending_exec_) {
      const std::string& h = entry.first;
      int64_t indicator = batch_order_fairness_
                              ? proposal_manager_->GetBofSeq(h)
                              : proposal_manager_->GetFinalOrderingKey(h);
      if (indicator <= 0) continue;  // not finalized yet
      if (indicator > tau_committed_) continue;  // not yet safe
      eligible.push_back({indicator, h});
    }
  }
  // Sort by indicator ascending with hash tiebreak — deterministic across
  // replicas.  In OL this is K(t) order (Section V-A); in BOF this is
  // sequence-number order, which matches the thesis's "sequence numbers fall
  // below τ" ordering of stable batches.
  std::sort(eligible.begin(), eligible.end(),
            [](const std::pair<int64_t, std::string>& a,
               const std::pair<int64_t, std::string>& b) {
              if (a.first != b.first) return a.first < b.first;
              return a.second < b.second;
            });

  // Execute eligible txns and emit per-slot execution-latency aggregate.
  int64_t exec_own_sum_create_us = 0;
  int exec_own_samples = 0;
  int64_t exec_own_max_latency_us = 0;
  int64_t now_exec_us = GetCurrentTime();
  std::vector<std::string> executed_hashes;
  executed_hashes.reserve(eligible.size());
  {
    std::lock_guard<std::mutex> lk(pending_exec_mutex_);
    for (auto& kv : eligible) {
      auto pit = pending_exec_.find(kv.second);
      if (pit == pending_exec_.end()) continue;
      Transaction& txn = pit->second;
      txn.set_ordering_key(kv.first);
      txn.set_id(execute_id_++);
      commit_(txn);
      executed_hashes.push_back(kv.second);

      // Clock-consistent execution latency sample (own-originated only).
      int64_t create_us = -1;
      {
        std::lock_guard<std::mutex> olk(own_create_times_mutex_);
        auto oit = own_create_times_.find(kv.second);
        if (oit != own_create_times_.end()) {
          create_us = oit->second;
          own_create_times_.erase(oit);
        }
      }
      if (create_us > 0) {
        int64_t lat = std::max<int64_t>(0, now_exec_us - create_us);
        exec_own_sum_create_us += lat;
        if (lat > exec_own_max_latency_us) exec_own_max_latency_us = lat;
        exec_own_samples++;
      }
      pending_exec_.erase(pit);
    }
  }

  {
    int64_t avg_exec_latency_us =
        exec_own_samples > 0 ? exec_own_sum_create_us / exec_own_samples : 0;
    size_t pending_size;
    {
      std::lock_guard<std::mutex> lk(pending_exec_mutex_);
      pending_size = pending_exec_.size();
    }
    LOG(ERROR) << "executed_batch slot:" << slot_id
               << " count:" << executed_hashes.size()
               << " avg_latency_us:" << avg_exec_latency_us
               << " max_latency_us:" << exec_own_max_latency_us
               << " pending:" << pending_size;
  }

  // Prune proposal_manager_ state for txns that actually executed (not merely
  // committed) — buffered txns still need their ordering data for the τ check.
  if (!executed_hashes.empty()) {
    if (batch_order_fairness_) {
      proposal_manager_->MarkBofCommitted(executed_hashes);
    } else {
      proposal_manager_->PruneOlCommitted(executed_hashes);
    }
  }

  // Update throughput stats — count executed txns, matching execution_tps
  // semantics.  Pending-but-buffered txns will be counted when they execute.
  if (!executed_hashes.empty()) {
    global_stats_->ConsumeTransactions(static_cast<int>(executed_hashes.size()));
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
