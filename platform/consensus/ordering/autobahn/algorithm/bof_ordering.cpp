#include "platform/consensus/ordering/autobahn/algorithm/bof_ordering.h"

#include <glog/logging.h>

#include <algorithm>
#include <cmath>
#include <queue>

namespace resdb {
namespace autobahn {

BofOrderer::BofOrderer(int n, int f, double gamma)
    : n_(n),
      f_(f),
      committed_(n + 1, 0),
      discarded_(n + 1, false),
      uncovered_(n + 1),
      unsettled_(n + 1) {
  // theta = ceil(gamma (n - f)); the epsilon absorbs float error on gamma.
  theta_ = static_cast<int>(std::ceil(gamma * (n - f) - 1e-9));
}

bool BofOrderer::Settled(const TxnState& t, int j) const {
  return discarded_[j] || committed_[j] >= t.hat[j];
}

BofCommitResult BofOrderer::Commit(const std::vector<BofCommittedCar>& cars,
                                   const std::vector<int>& claimed_lanes) {
  BofSettleResult settled = Settle(cars, claimed_lanes);
  BofCommitResult result;
  result.armed = std::move(settled.armed);
  if (!settled.eligible.empty()) {
    result.batches = Order(std::move(settled.eligible), n_, theta_);
  }
  return result;
}

BofSettleResult BofOrderer::Settle(const std::vector<BofCommittedCar>& cars,
                                   const std::vector<int>& claimed_lanes) {
  BofSettleResult result;

  // A lane named by a committed claim contributes no ordering report
  // thereafter, including through the Cars committed by this same block.
  for (int j : claimed_lanes) {
    if (discarded_[j]) continue;
    discarded_[j] = true;
    unsettled_[j].clear();
    uncovered_[j].clear();
  }

  std::vector<std::string> newly_fixed;
  for (const BofCommittedCar& car : cars) {
    const int r = car.lane;
    CHECK_EQ(car.index, committed_[r] + 1)
        << "lane " << r << " committed out of order";
    committed_[r] = car.index;
    // Lane r's committed prefix advanced: it now settles every pending
    // c_hat_r(t) <= car.index.
    unsettled_[r].erase(unsettled_[r].begin(),
                        unsettled_[r].upper_bound(car.index));
    if (discarded_[r]) continue;

    for (size_t p = 0; p < car.items.size(); ++p) {
      if (car.heartbeat[p]) continue;
      const std::string& id = car.items[p];
      if (executed_.count(id)) continue;
      TxnState& t = txns_[id];
      if (t.attest.empty()) {
        t.attest.assign(n_ + 1, kNoPos);
        t.hat.assign(n_ + 1, 0);
      }
      if (t.attest[r] != kNoPos) continue;
      t.attest[r] = Pos{car.index, static_cast<int32_t>(p)};
      uncovered_[r].emplace_back(car.index, id);
    }

    // Coverage: every committed item of lane r at a position <= w_r.
    auto& queue = uncovered_[r];
    while (!queue.empty() && queue.front().first <= car.w) {
      const std::string id = queue.front().second;
      queue.pop_front();
      auto it = txns_.find(id);
      if (it == txns_.end()) continue;  // already executed
      TxnState& t = it->second;
      if (t.hat_fixed) continue;
      t.coverers.push_back(r);
      for (int j = 1; j <= n_; ++j) {
        if (car.m[j].present) t.hat[j] = std::max(t.hat[j], car.m[j].car);
      }
      if (static_cast<int>(t.coverers.size()) == f_ + 1) {
        t.hat_fixed = true;
        newly_fixed.push_back(id);
      }
    }
  }

  // Lanes still unsettled for a newly fixed c_hat arm a delinquency timer.
  for (const std::string& id : newly_fixed) {
    const TxnState& t = txns_[id];
    hat_fixed_.insert(id);
    for (int j = 1; j <= n_; ++j) {
      if (Settled(t, j)) continue;
      unsettled_[j].insert(t.hat[j]);
      result.armed.emplace_back(j, t.hat[j]);
    }
  }

  // Eligible: c_hat fixed and every lane settled.  The edge rule counts
  // only lanes not discarded as of this block.
  for (auto it = hat_fixed_.begin(); it != hat_fixed_.end();) {
    auto st = txns_.find(*it);
    bool all = true;
    for (int j = 1; j <= n_ && all; ++j) all = Settled(st->second, j);
    if (!all) {
      ++it;
      continue;
    }
    BofEligibleTxn e;
    e.id = *it;
    e.attest = std::move(st->second.attest);
    for (int r = 1; r <= n_; ++r) {
      if (discarded_[r]) e.attest[r] = kNoPos;
    }
    result.eligible.push_back(std::move(e));
    executed_.insert(*it);
    txns_.erase(st);
    it = hat_fixed_.erase(it);
  }
  return result;
}

std::vector<std::vector<std::string>> BofOrderer::Order(
    std::vector<BofEligibleTxn> txns, int n, int theta) {
  std::sort(txns.begin(), txns.end(),
            [](const BofEligibleTxn& a, const BofEligibleTxn& b) {
              return a.id < b.id;
            });
  const int m = static_cast<int>(txns.size());

  // Dependency graph (Alg. 3, edge rule Eq. 2).
  std::vector<std::vector<int>> adj(m);
  for (int a = 0; a < m; ++a) {
    for (int b = a + 1; b < m; ++b) {
      int size = 0, ab = 0;
      for (int r = 1; r <= n; ++r) {
        const auto& pa = txns[a].attest[r];
        const auto& pb = txns[b].attest[r];
        if (pa.first < 0 || pb.first < 0) continue;
        ++size;
        if (pa < pb) ++ab;
      }
      if (ab >= theta) {
        adj[a].push_back(b);
      } else if (size - ab >= theta) {
        adj[b].push_back(a);
      }
    }
  }

  // Tarjan's SCC algorithm, iterative to bound stack depth.
  std::vector<int> index(m, -1), low(m, 0), comp(m, -1);
  std::vector<bool> on_stack(m, false);
  std::vector<int> stack;
  int next_index = 0, num_comp = 0;
  for (int root = 0; root < m; ++root) {
    if (index[root] != -1) continue;
    std::vector<std::pair<int, size_t>> call = {{root, 0}};
    index[root] = low[root] = next_index++;
    stack.push_back(root);
    on_stack[root] = true;
    while (!call.empty()) {
      int v = call.back().first;
      size_t& e = call.back().second;
      if (e < adj[v].size()) {
        int w = adj[v][e++];
        if (index[w] == -1) {
          index[w] = low[w] = next_index++;
          stack.push_back(w);
          on_stack[w] = true;
          call.emplace_back(w, 0);
        } else if (on_stack[w]) {
          low[v] = std::min(low[v], index[w]);
        }
        continue;
      }
      if (low[v] == index[v]) {
        int w;
        do {
          w = stack.back();
          stack.pop_back();
          on_stack[w] = false;
          comp[w] = num_comp;
        } while (w != v);
        ++num_comp;
      }
      call.pop_back();
      if (!call.empty()) {
        int parent = call.back().first;
        low[parent] = std::min(low[parent], low[v]);
      }
    }
  }

  // Condensation DAG; txns are sorted, so the first member of a component is
  // its lexicographically smallest digest.
  std::vector<std::vector<std::string>> members(num_comp);
  std::vector<int> smallest(num_comp, -1);
  for (int i = 0; i < m; ++i) {
    members[comp[i]].push_back(std::move(txns[i].id));
    if (smallest[comp[i]] == -1) smallest[comp[i]] = i;
  }
  std::vector<std::set<int>> cadj(num_comp);
  std::vector<int> indeg(num_comp, 0);
  for (int u = 0; u < m; ++u) {
    for (int v : adj[u]) {
      if (comp[u] != comp[v] && cadj[comp[u]].insert(comp[v]).second) {
        ++indeg[comp[v]];
      }
    }
  }

  // Deterministic topological sort: among ready components, the one with the
  // smallest digest first.
  using Entry = std::pair<int, int>;  // (smallest member index, component)
  std::priority_queue<Entry, std::vector<Entry>, std::greater<Entry>> ready;
  for (int c = 0; c < num_comp; ++c) {
    if (indeg[c] == 0) ready.emplace(smallest[c], c);
  }
  std::vector<std::vector<std::string>> batches;
  batches.reserve(num_comp);
  while (!ready.empty()) {
    int c = ready.top().second;
    ready.pop();
    batches.push_back(std::move(members[c]));
    for (int d : cadj[c]) {
      if (--indeg[d] == 0) ready.emplace(smallest[d], d);
    }
  }
  return batches;
}

std::map<int, int64_t> BofOrderer::ClaimTargets() const {
  std::map<int, int64_t> targets;
  for (int j = 1; j <= n_; ++j) {
    if (discarded_[j] || unsettled_[j].empty()) continue;
    targets[j] = *unsettled_[j].begin();
  }
  return targets;
}

}  // namespace autobahn
}  // namespace resdb
