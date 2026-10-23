#pragma once

#include <atomic>
#include <cstdint>

namespace resdb {

// Process-wide byte counters of the replica network layer: bytes read from
// incoming connections and bytes queued on outgoing ones.  Used for
// admission control (link utilisation), where interface counters are too
// coarse-grained.
struct NetworkTraffic {
  static std::atomic<int64_t>& RxBytes() {
    static std::atomic<int64_t> rx{0};
    return rx;
  }
  static std::atomic<int64_t>& TxBytes() {
    static std::atomic<int64_t> tx{0};
    return tx;
  }
  // Part of RxBytes received from clients.
  static std::atomic<int64_t>& ClientRxBytes() {
    static std::atomic<int64_t> rx{0};
    return rx;
  }
};

}  // namespace resdb
