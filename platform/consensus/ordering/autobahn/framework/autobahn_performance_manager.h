#pragma once

#include "platform/consensus/ordering/common/framework/performance_manager.h"

namespace resdb {
namespace autobahn {

// Pearl client: correct clients send each of their transactions to every
// replica (paper Sec. 5.1), so every replica attests it on first reception.
// The request is serialized and signed once and broadcast to all replicas.
class AutobahnPerformanceManager : public common::PerformanceManager {
 public:
  AutobahnPerformanceManager(const ResDBConfig& config,
                             ReplicaCommunicator* replica_communicator,
                             SignatureVerifier* verifier)
      : PerformanceManager(config, replica_communicator, verifier) {}

 protected:
  void SendMessage(const Request& request) override {
    replica_communicator_->BroadCast(request);
  }
};

}  // namespace autobahn
}  // namespace resdb
