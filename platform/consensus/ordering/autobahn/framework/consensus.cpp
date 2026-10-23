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
    default:
      LOG(ERROR) << "unknown Pearl message type " << request->user_type();
      return -1;
  }
  return 0;
}

int Consensus::ProcessNewTransaction(std::unique_ptr<Request> request) {
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
  // Execution throughput and latency are measured by the replicas (Execute()
  // logs `executed_batch`).  The benchmark client sends open-loop at its
  // target rate (das.py sets max_process_txn above any run's volume), so no
  // reply is sent back.
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
