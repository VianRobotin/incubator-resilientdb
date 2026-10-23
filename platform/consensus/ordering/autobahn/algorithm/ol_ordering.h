#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace resdb {
namespace autobahn {

// A Car of Pearl-OL as committed by consensus: the local ordering keys K_r(t)
// it carries and the completion frontier G_r sealed into it (paper Sec. 5.2).
struct OlCommittedCar {
  int lane = 0;
  int64_t index = 0;
  int64_t frontier = 0;
  std::vector<std::pair<std::string, int64_t>> items;  // (id(t), K_lane(t))
};

// Pearl-OL execution rule (paper Sec. 6), evaluated on committed Cars only.
//
//  - Final ordering key: K(t) = min over the first f+1 committed local keys
//    for t (Sec. 6.1).  Cars must be fed in commitment order (block height,
//    then replica identifier, then position in the block).
//  - Execution threshold (Eq. tau, Sec. 6.2):
//        tau = min( G^(f+1), min_{t in U} K_(t) )
//    where G^(f+1) is the (f+1)-th smallest committed frontier, U the set of
//    committed transactions with fewer than f+1 local keys, and K_(t) the
//    (f+1)-th smallest of T_j(t) = G_j^(i-1) if lane j published K_j(t) in
//    Car i, and G_j otherwise.
//  - A transaction executes once K(t) is fixed and K(t) < tau (strict),
//    in increasing K(t) with ties broken by digest.
class OlOrderer {
 public:
  OlOrderer(int n, int f);

  // Processes the Cars newly committed by one block, in commitment order.
  // Returns the transactions that became eligible, in execution order.
  std::vector<std::string> Commit(const std::vector<OlCommittedCar>& cars);

  int64_t Threshold() const { return tau_; }
  // Committed, not yet executed transactions (fixed or not).
  size_t PendingSize() const { return txns_.size(); }
  int64_t CommittedIndex(int lane) const { return committed_index_[lane]; }

 private:
  struct TxnState {
    std::vector<std::pair<int, int64_t>> published;  // (lane, Car index)
    std::vector<int64_t> keys;
  };

  int64_t KthSmallest(std::vector<int64_t> values) const;

  int n_, f_;
  std::vector<int64_t> committed_index_;               // per lane
  std::vector<int64_t> frontier_;                      // G_j
  std::vector<std::map<int64_t, int64_t>> car_frontier_;  // lane: index -> G
  std::unordered_map<std::string, TxnState> txns_;
  std::unordered_set<std::string> unfixed_;            // U
  std::set<std::pair<int64_t, std::string>> fixed_;    // (K(t), id), K fixed
  std::unordered_set<std::string> executed_;
  int64_t tau_ = 0;
};

}  // namespace autobahn
}  // namespace resdb
