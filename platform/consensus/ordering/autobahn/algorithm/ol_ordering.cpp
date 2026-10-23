#include "platform/consensus/ordering/autobahn/algorithm/ol_ordering.h"

#include <glog/logging.h>

#include <algorithm>

namespace resdb {
namespace autobahn {

OlOrderer::OlOrderer(int n, int f)
    : n_(n),
      f_(f),
      committed_index_(n + 1, 0),
      frontier_(n + 1, 0),
      car_frontier_(n + 1) {}

int64_t OlOrderer::KthSmallest(std::vector<int64_t> values) const {
  std::nth_element(values.begin(), values.begin() + f_, values.end());
  return values[f_];
}

std::vector<std::string> OlOrderer::Commit(
    const std::vector<OlCommittedCar>& cars) {
  for (const OlCommittedCar& car : cars) {
    // Committing a tip commits the lane prefix, so Cars arrive in order.
    CHECK_EQ(car.index, committed_index_[car.lane] + 1)
        << "lane " << car.lane << " committed out of order";
    committed_index_[car.lane] = car.index;
    car_frontier_[car.lane][car.index] = car.frontier;
    frontier_[car.lane] = std::max(frontier_[car.lane], car.frontier);

    for (const auto& item : car.items) {
      const std::string& id = item.first;
      if (executed_.count(id)) continue;
      TxnState& t = txns_[id];
      if (static_cast<int>(t.keys.size()) == f_ + 1) continue;  // K(t) fixed
      bool dup = false;
      for (const auto& p : t.published) dup |= p.first == car.lane;
      if (dup) continue;
      t.published.emplace_back(car.lane, car.index);
      t.keys.push_back(item.second);
      if (static_cast<int>(t.keys.size()) == f_ + 1) {
        unfixed_.erase(id);
        fixed_.emplace(*std::min_element(t.keys.begin(), t.keys.end()), id);
      } else {
        unfixed_.insert(id);
      }
    }
  }

  // tau = min(G^(f+1), min_{t in U} K_(t)).
  std::vector<int64_t> g(frontier_.begin() + 1, frontier_.end());
  int64_t tau = KthSmallest(g);
  std::vector<int64_t> lower(n_);
  for (const std::string& id : unfixed_) {
    const TxnState& t = txns_[id];
    for (int j = 1; j <= n_; ++j) lower[j - 1] = frontier_[j];
    for (const auto& p : t.published) {
      // G_j^(i-1): the frontier of the Car preceding the one carrying K_j(t).
      auto it = car_frontier_[p.first].find(p.second - 1);
      lower[p.first - 1] = it == car_frontier_[p.first].end() ? 0 : it->second;
    }
    tau = std::min(tau, KthSmallest(lower));
  }
  tau_ = tau;

  std::vector<std::string> eligible;
  while (!fixed_.empty() && fixed_.begin()->first < tau_) {
    const std::string id = fixed_.begin()->second;
    fixed_.erase(fixed_.begin());
    txns_.erase(id);
    executed_.insert(id);
    eligible.push_back(id);
  }
  return eligible;
}

}  // namespace autobahn
}  // namespace resdb
