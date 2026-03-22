#pragma once

#include "platform/consensus/ordering/common/framework/performance_manager.h"

namespace resdb {
namespace autobahn {

// Autobahn-specific performance manager that distributes transactions
// round-robin across ALL replicas, not just the primary.
// This is required because Autobahn is a multi-lane protocol where each
// replica generates its own blocks from transactions it receives.
class AutobahnPerformanceManager : public common::PerformanceManager {
 public:
  AutobahnPerformanceManager(const ResDBConfig& config,
                             ReplicaCommunicator* replica_communicator,
                             SignatureVerifier* verifier)
      : PerformanceManager(config, replica_communicator, verifier),
        replica_num_(config.GetReplicaNum()),
        round_robin_idx_(0) {}

 protected:
  void SendMessage(const Request& request) override {
    int target = (round_robin_idx_++ % replica_num_) + 1;
    LOG(ERROR) << "AutobahnPM sending to replica " << target
               << " (round-robin idx=" << round_robin_idx_.load() << ")";
    replica_communicator_->SendMessage(request, target);
  }

 private:
  int replica_num_;
  std::atomic<int> round_robin_idx_;
};

}  // namespace autobahn
}  // namespace resdb
