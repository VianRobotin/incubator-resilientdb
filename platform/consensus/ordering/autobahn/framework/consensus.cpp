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
#include "platform/consensus/ordering/autobahn/framework/autobahn_performance_manager.h"

#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>

#include "common/crypto/signature_verifier.h"
#include "common/utils/utils.h"
#include "platform/proto/resdb.pb.h"
#include "proto/kv/kv.pb.h"

namespace resdb {
namespace autobahn {

Consensus::Consensus(const ResDBConfig& config,
                     std::unique_ptr<TransactionManager> executor)
    : common::Consensus(config, std::move(executor)){
  int total_replicas = config_.GetReplicaNum();
  // Sync HotStuff operates in a synchronous network with 2f+1 replicas.
  // f = (n-1)/2 (tolerates minority Byzantine faults under synchrony).
  //
  // `fault_num` lets the operator pin f independently of n, so Pearl can be
  // deployed on a deliberately oversized committee (e.g. a 3f+1 = n committee
  // at the baselines' fault tolerance) instead of always claiming the maximal
  // f=(n-1)/2. Unset => the native 2f+1 behaviour. Clamp to [0, (n-1)/2] so a
  // bad value can never produce a 2f+1 quorum larger than the committee.
  int f = (total_replicas - 1) / 2;
  if (config_.GetConfigData().has_fault_num() &&
      config_.GetConfigData().fault_num() > 0) {
    int pinned = config_.GetConfigData().fault_num();
    f = std::min(pinned, (total_replicas - 1) / 2);
  }

  start_ = 0;

  bool is_client = config_.GetPublicKeyCertificateInfo()
                       .public_key()
                       .public_key_info()
                       .type() == CertificateKeyInfo::CLIENT;

  // CLIENT cert nodes use AutobahnPerformanceManager, which sends transactions
  // round-robin across all replica lanes (correct for a multi-lane DAG protocol).
  // Must be set BEFORE Init() so Init() skips creating the base PerformanceManager.
  // If Init() runs first it creates a base PerformanceManager whose constructor
  // spawns BatchProposeMsg (blocking on eval_ready_future_), then SetPerformanceManager
  // destroys it, which joins the blocked thread — deadlock.
  if (is_client) {
    SetPerformanceManager(std::make_unique<AutobahnPerformanceManager>(
        config_, GetBroadCastClient(), GetSignatureVerifier()));
  }

  Init();

  if (!is_client) {
    const auto& cfg = config_.GetConfigData();
    AutoBahn::Params params;
    params.batch_order_fairness = cfg.batch_order_fairness();
    // gamma in (1/2, 1] for BOF; default 1.0 (n > 2 gamma f / (2 gamma - 1)).
    params.bof_gamma = cfg.has_bof_gamma() ? cfg.bof_gamma() : 1.0f;
    params.delta_us =
        1000LL * (cfg.has_delta_ms() && cfg.delta_ms() > 0 ? cfg.delta_ms() : 50);
    params.heartbeat_us = cfg.has_heartbeat_ms() && cfg.heartbeat_ms() > 0
                              ? 1000LL * cfg.heartbeat_ms()
                              : params.delta_us;
    params.blame_p = cfg.has_blame_p() && cfg.blame_p() > 0 ? cfg.blame_p() : 1;
    params.link_bytes_per_sec =
        125000LL * (cfg.has_link_mbps() && cfg.link_mbps() > 0 ? cfg.link_mbps() : 1000);
    params.dissemination_threads =
        cfg.has_dissemination_threads() && cfg.dissemination_threads() > 0
            ? cfg.dissemination_threads()
            : 4;
    const char* enclave = getenv("TEE_ENCLAVE_PATH");
    params.enclave_path =
        enclave ? enclave
                : "platform/consensus/ordering/autobahn/tee/tee_enclave.signed.so";
    // Committee authentication key of the enclaves.  On SGX hardware it would
    // be provisioned after remote attestation; in SIM mode every replica
    // derives it from the same seed.
    const char* seed = getenv("PEARL_GROUP_SEED");
    params.group_seed =
        SignatureVerifier::CalculateHash(seed ? seed : "pearl-sim-group-seed");

    autobahn_ = std::make_unique<AutoBahn>(
        config_.GetSelfInfo().id(), f, total_replicas, cfg.block_size(),
        GetSignatureVerifier(), params);
    InitProtocol(autobahn_.get());

    // Latency of the send path: a batch is sent once full or after this.
    const int batch_timeout_us =
        cfg.has_tcp_batch_timeout_us() ? cfg.tcp_batch_timeout_us() : 0;
    if (batch_timeout_us > 0) GetBroadCastClient()->SetBatchTimeoutUs(batch_timeout_us);
    LOG(ERROR) << "Pearl: send batch timeout " << batch_timeout_us << "us";
    // Echo batches are large: batch only a few per write.
    bulk_communicator_ = std::make_unique<ReplicaCommunicator>(
        config_.GetReplicaInfos(), GetSignatureVerifier(),
        /*is_use_long_conn=*/true, config_.GetOutputWorkerNum(),
        /*tcp_batch=*/4);
    if (batch_timeout_us > 0) bulk_communicator_->SetBatchTimeoutUs(batch_timeout_us);
    autobahn_->SetBulkChannel(
        [this](int type, const google::protobuf::Message& msg) {
          Request request;
          msg.SerializeToString(request.mutable_data());
          request.set_type(Request::TYPE_CUSTOM_CONSENSUS);
          request.set_user_type(type);
          request.set_sender_id(config_.GetSelfInfo().id());
          bulk_communicator_->BroadCast(request);
        },
        [this]() {
          return bulk_communicator_->PendingBytes() +
                 GetBroadCastClient()->PendingBytes();
        });
    autobahn_->Start();
  }
}

namespace {
template <typename T>
std::unique_ptr<T> Parse(const Request& request) {
  auto msg = std::make_unique<T>();
  if (!msg->ParseFromString(request.data())) {
    LOG(ERROR) << "parse " << MessageType_Name(
                      static_cast<MessageType>(request.user_type()))
               << " fail";
    return nullptr;
  }
  return msg;
}
}  // namespace

int Consensus::ProcessCustomConsensus(std::unique_ptr<Request> request) {
  switch (request->user_type()) {
    case MessageType::Echo_Batch:
      if (auto m = Parse<EchoBatch>(*request)) autobahn_->ReceiveEchoBatch(std::move(m));
      break;
    case MessageType::Car_Prop:
      if (auto m = Parse<Car>(*request)) autobahn_->ReceiveCar(std::move(m));
      break;
    case MessageType::Car_Ack:
      if (auto m = Parse<CarAck>(*request)) autobahn_->ReceiveCarAck(std::move(m));
      break;
    case MessageType::Car_Query:
      if (auto m = Parse<CarQuery>(*request)) autobahn_->ReceiveCarQuery(std::move(m));
      break;
    case MessageType::Body_Query:
      if (auto m = Parse<BodyQuery>(*request)) autobahn_->ReceiveBodyQuery(std::move(m));
      break;
    case MessageType::Body_Reply:
      if (auto m = Parse<BodyReply>(*request)) autobahn_->ReceiveBodyReply(std::move(m));
      break;
    case MessageType::Block_Query:
      if (auto m = Parse<BlockQuery>(*request)) autobahn_->ReceiveBlockQuery(std::move(m));
      break;
    case MessageType::Block_Reply:
      if (auto m = Parse<BlockReply>(*request)) autobahn_->ReceiveBlockReply(std::move(m));
      break;
    case MessageType::SyncHS_Propose:
      if (auto m = Parse<Proposal>(*request)) autobahn_->ReceiveProposal(std::move(m));
      break;
    case MessageType::SyncHS_Vote:
      if (auto m = Parse<Vote>(*request)) autobahn_->ReceiveVote(std::move(m));
      break;
    case MessageType::SyncHS_Blame:
      if (auto m = Parse<Blame>(*request)) autobahn_->ReceiveBlame(std::move(m));
      break;
    case MessageType::SyncHS_QuitView:
      if (auto m = Parse<QuitView>(*request)) autobahn_->ReceiveQuitView(std::move(m));
      break;
    case MessageType::SyncHS_Status:
      if (auto m = Parse<Status>(*request)) autobahn_->ReceiveStatus(std::move(m));
      break;
    case MessageType::Poa_Forward:
      if (auto m = Parse<PoaForward>(*request)) autobahn_->ReceivePoaForward(std::move(m));
      break;
    case MessageType::Lane_Suspect:
      if (auto m = Parse<Suspect>(*request)) autobahn_->ReceiveSuspect(std::move(m));
      break;
    default:
      LOG(ERROR) << "unknown Pearl message type " << request->user_type();
      return -1;
  }
  return 0;
}

int Consensus::ProcessNewTransaction(std::unique_ptr<Request> request) {
  return autobahn_->ReceiveTransaction(std::move(*request->mutable_data()));
}

int Consensus::CommitMsg(const google::protobuf::Message& msg) {
  return CommitMsgInternal(dynamic_cast<const Transaction&>(msg));
}

// Executes a transaction on the application (the KV store) when Pearl's
// execution layer releases it, in Pearl's order.  Throughput and latency are
// measured by the replicas (`executed_batch`); the benchmark client sends
// open-loop at its target rate, so no reply is sent back.
int Consensus::CommitMsgInternal(const Transaction& txn) {
  BatchUserRequest batch;
  if (!batch.ParseFromString(txn.data())) {
    LOG(ERROR) << "Pearl: cannot parse the client request of transaction " << txn.id();
    return -1;
  }
  TransactionManager* app = transaction_executor_->GetTransactionManager();
  if (app != nullptr) app->ExecuteBatch(batch);
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
