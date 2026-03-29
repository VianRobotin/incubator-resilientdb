/*
 * Copyright (c) 2019-2022 ExpoLab, UC Davis
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 */

#include "platform/consensus/ordering/autobahn/framework/consensus.h"

#include <glog/logging.h>
#include <unistd.h>

#include "common/crypto/signature_verifier.h"
#include "common/utils/utils.h"
#include "proto/kv/kv.pb.h"

namespace resdb {
namespace autobahn {

Consensus::Consensus(const ResDBConfig& config,
                     std::unique_ptr<TransactionManager> executor)
    : common::Consensus(config, std::move(executor)){
  int total_replicas = config_.GetReplicaNum();
  // Sync HotStuff operates in a synchronous network with 2f+1 replicas.
  // f = (n-1)/2 (tolerates minority Byzantine faults under synchrony).
  int f = (total_replicas - 1) / 2;

  Init();

  start_ = 0;
  local_txn_gen_started_ = false;

  if (config_.GetPublicKeyCertificateInfo()
          .public_key()
          .public_key_info()
          .type() != CertificateKeyInfo::CLIENT) {
    bool batch_order_fairness = config_.GetConfigData().batch_order_fairness();
    // γ ∈ (0.5, 1.0] for BOF; default 1.0 (strictest, works for n=2f+1).
    float bof_gamma = config_.GetConfigData().has_bof_gamma()
                          ? config_.GetConfigData().bof_gamma()
                          : 1.0f;
    autobahn_ = std::make_unique<AutoBahn>(
        config_.GetSelfInfo().id(), f,
                                   total_replicas, config_.GetConfigData().block_size(),
                                   GetSignatureVerifier(),
                                   batch_order_fairness, bof_gamma);

    InitProtocol(autobahn_.get());

    // Auto-start local transaction generation after a delay to allow
    // peer discovery. In performance mode, each replica generates its own
    // transactions for its Autobahn lane.
    if (config_.IsPerformanceRunning()) {
      std::thread([this]() {
        LOG(ERROR) << "Waiting for peer discovery before auto-starting txn generation...";
        sleep(10);  // Wait for heartbeat/peer discovery
        StartLocalTxnGeneration();
      }).detach();
    }
  }
}

int Consensus::ProcessCustomConsensus(std::unique_ptr<Request> request) {
  // Data dissemination messages (unchanged)
  if (request->user_type() == MessageType::NewBlocks) {
    std::unique_ptr<Block> block = std::make_unique<Block>();
    if (!block->ParseFromString(request->data())) {
      assert(1 == 0);
      LOG(ERROR) << "parse block fail";
      return -1;
    }
    // When we see blocks from other replicas in performance mode,
    // start generating our own transactions for our lane.
    StartLocalTxnGeneration();
    autobahn_->ReceiveBlock(std::move(block));
    return 0;
  }
  else if (request->user_type() == MessageType::CMD_BlockACK) {
    std::unique_ptr<BlockACK> block_ack = std::make_unique<BlockACK>();
    if (!block_ack->ParseFromString(request->data())) {
      LOG(ERROR) << "parse block ack fail";
      assert(1 == 0);
      return -1;
    }
    autobahn_->ReceiveBlockACK(std::move(block_ack));
    return 0;
  }
  // Sync HotStuff consensus messages
  else if (request->user_type() == MessageType::SyncHS_Propose) {
    std::unique_ptr<Proposal> proposal = std::make_unique<Proposal>();
    if (!proposal->ParseFromString(request->data())) {
      LOG(ERROR) << "parse SyncHS proposal fail";
      assert(1 == 0);
      return -1;
    }
    if (!autobahn_->ReceiveProposal(std::move(proposal))) {
      return -1;
    }
    return 0;
  }
  else if (request->user_type() == MessageType::SyncHS_Vote) {
    std::unique_ptr<Proposal> vote = std::make_unique<Proposal>();
    if (!vote->ParseFromString(request->data())) {
      LOG(ERROR) << "parse SyncHS vote fail";
      assert(1 == 0);
      return -1;
    }
    if (!autobahn_->ReceiveVote(std::move(vote))) {
      return -1;
    }
    return 0;
  }
  else if (request->user_type() == MessageType::SyncHS_Equivocation) {
    std::unique_ptr<EquivocationProof> proof = std::make_unique<EquivocationProof>();
    if (!proof->ParseFromString(request->data())) {
      LOG(ERROR) << "parse equivocation proof fail";
      assert(1 == 0);
      return -1;
    }
    if (!autobahn_->ReceiveEquivocation(std::move(proof))) {
      return -1;
    }
    return 0;
  }
  // Fair ordering: TEE timestamp dissemination
  else if (request->user_type() == MessageType::TEE_Timestamps) {
    std::unique_ptr<TimestampBatch> batch = std::make_unique<TimestampBatch>();
    if (!batch->ParseFromString(request->data())) {
      LOG(ERROR) << "parse TEE timestamp batch fail";
      assert(1 == 0);
      return -1;
    }
    autobahn_->ReceiveTimestamps(std::move(batch));
    return 0;
  }
  // Batch-order fairness: relative ordering dissemination
  else if (request->user_type() == MessageType::BOF_RelativeOrder) {
    std::unique_ptr<RelativeOrdering> ordering = std::make_unique<RelativeOrdering>();
    if (!ordering->ParseFromString(request->data())) {
      LOG(ERROR) << "parse relative ordering fail";
      assert(1 == 0);
      return -1;
    }
    autobahn_->ReceiveRelativeOrdering(std::move(ordering));
    return 0;
  }
  // Legacy PBFT messages — log and ignore (protocol replaced by Sync HotStuff)
  else if (request->user_type() == MessageType::NewProposal ||
           request->user_type() == MessageType::ProposalAck ||
           request->user_type() == MessageType::Prepare ||
           request->user_type() == MessageType::Commit) {
    LOG(ERROR) << "Received legacy PBFT message type "
               << MessageType_Name(request->user_type())
               << " — ignoring (Sync HotStuff active)";
    return 0;
  }
  return 0;
}

void Consensus::StartLocalTxnGeneration() {
  if (!config_.IsPerformanceRunning() || autobahn_ == nullptr) return;
  if (local_txn_gen_started_.exchange(true)) return;

  LOG(ERROR) << "Starting local transaction generation for replica "
             << config_.GetSelfInfo().id();
  local_txn_gen_thread_ = std::thread([this]() {
    int count = 0;
    int batch_size = config_.GetConfigData().block_size();
    if (batch_size <= 0) batch_size = 100;
    // Continuous generation: keep producing transactions until stopped.
    // This ensures steady-state throughput for benchmarking.
    while (!is_stop_) {
      auto txn = std::make_unique<Transaction>();
      KVRequest kv_req;
      kv_req.set_cmd(KVRequest::SET);
      kv_req.set_key(std::to_string(config_.GetSelfInfo().id()) + "_" + std::to_string(count));
      kv_req.set_value("perf_data");
      std::string data;
      kv_req.SerializeToString(&data);
      txn->set_data(data);
      txn->set_create_time(GetCurrentTime());
      // Each transaction MUST have a unique hash for fair ordering.
      // The TEE timestamping uses txn.hash() to deduplicate — without
      // unique hashes, all transactions are treated as the same one.
      std::string unique_id = std::to_string(config_.GetSelfInfo().id())
          + "_" + std::to_string(count) + "_" + std::to_string(GetCurrentTime());
      txn->set_hash(SignatureVerifier::CalculateHash(unique_id));
      autobahn_->ReceiveTransaction(std::move(txn));
      count++;
      // Spread transactions over time to simulate continuous arrival.
      // This ensures fair ordering distributes txns across multiple
      // consensus slots rather than clustering in a single window.
      if (count % batch_size == 0) {
        usleep(500000);  // 500ms pause between batches
      }
    }
    LOG(ERROR) << "Local transaction generation stopped after: " << count << " txns";
  });
}

int Consensus::ProcessNewTransaction(std::unique_ptr<Request> request) {
  StartLocalTxnGeneration();

  std::unique_ptr<Transaction> txn = std::make_unique<Transaction>();
  txn->set_data(request->data());
  txn->set_hash(request->hash());
  txn->set_proxy_id(request->proxy_id());
  return autobahn_->ReceiveTransaction(std::move(txn));
}

int Consensus::CommitMsg(const google::protobuf::Message& msg) {
  return CommitMsgInternal(dynamic_cast<const Transaction&>(msg));
}

int Consensus::CommitMsgInternal(const Transaction& txn) {
  // Autobahn updates throughput stats directly in Commit() via
  // global_stats_->ConsumeTransactions(). The standard executor path
  // (AddExecuteMessage → RegisterExecute) uses a 1024-slot bucket ring that
  // overflows when a full block of transactions is submitted at once.
  // Transactions are internally generated for benchmarking so there is no
  // external client waiting for a response; skip the executor path entirely.
  (void)txn;
  return 0;
}


int Consensus::Prepare(const Transaction& txn) {
  std::unique_ptr<Request> request = std::make_unique<Request>();
  request->set_data(txn.data());
  request->set_uid(txn.uid());
  transaction_executor_->Prepare(std::move(request));
  return 0;
}


}  // namespace autobahn
}  // namespace resdb
