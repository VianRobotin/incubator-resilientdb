#pragma once

#include "platform/consensus/ordering/common/framework/performance_manager.h"

namespace resdb {
namespace autobahn {

// Pearl client: correct clients send each of their transactions to every
// replica (paper Sec. 5.1), so every replica attests it on first reception.
class AutobahnPerformanceManager : public common::PerformanceManager {
 public:
  AutobahnPerformanceManager(const ResDBConfig& config,
                             ReplicaCommunicator* replica_communicator,
                             SignatureVerifier* verifier)
      : PerformanceManager(config, replica_communicator, verifier),
        replica_num_(config.GetReplicaNum()) {}

 protected:
  void SendMessage(const Request& request) override {
    for (int target = 1; target <= replica_num_; ++target) {
      replica_communicator_->SendMessage(request, target);
    }
  }

 private:
  int replica_num_;
};

}  // namespace autobahn
}  // namespace resdb
