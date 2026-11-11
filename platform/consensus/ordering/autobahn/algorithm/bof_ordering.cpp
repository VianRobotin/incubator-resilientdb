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
      unsettled_(n + 1),
      hold_(n, 0) {
  // theta = ceil(gamma (n - f)); the epsilon absorbs float error on gamma.
  theta_ = static_cast<int>(std::ceil(gamma * (n - f) - 1e-9));
  hold_ = BofHoldBack(n, theta_);
}

bool BofOrderer::Settled(const TxnState& t, int j) const {
  return discarded_[j] || committed_[j] >= t.hat[j];
}

BofCommitResult BofOrderer::Commit(const std::vector<BofCommittedCar>& cars,
                                   const std::vector<int>& claimed_lanes) {
  BofSettleResult settled = Settle(cars, claimed_lanes);
  BofCommitResult result;
  result.armed = std::move(settled.armed);
  hold_.OnCommitted(cars, claimed_lanes);
  std::vector<BofEligibleTxn> released = hold_.Release(std::move(settled.eligible));
  if (!released.empty()) result.batches = Order(std::move(released), n_, theta_);
  return result;
}

BofHoldBack::BofHoldBack(int n, int theta)
    : n_(n), theta_(theta), discarded_(n + 1, false), lane_(n + 1) {}

uint32_t BofHoldBack::Intern(const std::string& id) {
  auto it = index_.emplace(id, static_cast<uint32_t>(state_.size()));
  if (it.second) state_.push_back(kPending);
  return it.first->second;
}

void BofHoldBack::OnCommitted(const std::vector<BofCommittedCar>& cars,
                              const std::vector<int>& claimed_lanes) {
  for (int j : claimed_lanes) {
    discarded_[j] = true;
    lane_[j].clear();
  }
  for (const BofCommittedCar& car : cars) {
    const int r = car.lane;
    if (discarded_[r]) continue;
    for (size_t p = 0; p < car.items.size(); ++p) {
      if (car.heartbeat[p]) continue;
      uint32_t t = Intern(car.items[p]);
      if (state_[t] == kExecuted) continue;
      // Lanes commit in order, so positions arrive increasing.
      lane_[r].push_back(Entry{Pos{car.index, static_cast<int32_t>(p)}, t});
    }
  }
}

const BofHoldBack::Entry* BofHoldBack::FirstPending(int lane) {
  auto& q = lane_[lane];
  while (!q.empty() && state_[q.front().txn] == kExecuted) q.pop_front();
  for (const Entry& e : q) {
    if (state_[e.txn] == kPending || state_[e.txn] == kHeld) return &e;
  }
  return nullptr;
}

int BofHoldBack::CountPreceding(const std::vector<Pos>& attest,
                                std::unordered_map<uint32_t, int>* before) {
  before->clear();
  int lanes = 0;
  for (int r = 1; r <= n_; ++r) {
    if (discarded_[r] || attest[r].first < 0) continue;
    ++lanes;
    for (const Entry& e : lane_[r]) {
      if (!(e.pos < attest[r])) break;
      if (state_[e.txn] != kExecuted) ++(*before)[e.txn];
    }
  }
  return lanes;
}

std::vector<BofEligibleTxn> BofHoldBack::Release(std::vector<BofEligibleTxn> eligible) {
  std::vector<BofEligibleTxn> candidates = std::move(held_);
  held_.clear();
  for (auto& e : eligible) candidates.push_back(std::move(e));
  std::vector<uint32_t> txn(candidates.size());
  for (size_t i = 0; i < candidates.size(); ++i) {
    txn[i] = Intern(candidates[i].id);
    state_[txn[i]] = kCandidate;
  }
  // Fast path: hold a candidate back if some pending (not eligible) or held
  // transaction blocks it, i.e. precedes it in at least theta counted lanes,
  // and iterate to a fixpoint.  Every candidate it keeps has only eligible
  // blockers, all of them kept too, so its closure D(t) is eligible: the
  // fast path releases a subset of what the closure rule releases.
  std::vector<bool> held(candidates.size(), false);
  std::vector<const Entry*> first(n_ + 1);
  std::unordered_map<uint32_t, int> before;
  for (bool changed = true; changed;) {
    changed = false;
    for (int r = 1; r <= n_; ++r) first[r] = discarded_[r] ? nullptr : FirstPending(r);
    for (size_t i = 0; i < candidates.size(); ++i) {
      if (held[i]) continue;
      const auto& attest = candidates[i].attest;
      int lanes = 0;
      for (int r = 1; r <= n_; ++r) {
        if (first[r] != nullptr && attest[r].first >= 0 && first[r]->pos < attest[r]) ++lanes;
      }
      if (lanes < theta_) continue;  // the common case
      before.clear();
      bool blocked = false;
      for (int r = 1; r <= n_ && !blocked; ++r) {
        if (first[r] == nullptr || attest[r].first < 0) continue;
        for (const Entry& e : lane_[r]) {
          if (!(e.pos < attest[r])) break;
          if ((state_[e.txn] == kPending || state_[e.txn] == kHeld) &&
              ++before[e.txn] >= theta_) {
            blocked = true;
            break;
          }
        }
      }
      if (!blocked) continue;
      held[i] = true;
      state_[txn[i]] = kHeld;
      changed = true;
    }
  }

  // Closure rule for the candidates the fast path held.  D(t) is computed on
  // the state before this step for every t, so the released set does not
  // depend on the order of the candidates.  Expansion stops at a candidate
  // the fast path kept: its blockers are all eligible and kept.
  std::unordered_map<uint32_t, size_t> held_index;
  for (size_t i = 0; i < candidates.size(); ++i) {
    if (held[i]) held_index.emplace(txn[i], i);
  }
  std::vector<bool> release(candidates.size(), false);
  std::unordered_map<uint32_t, int> ahead, blockers;
  std::vector<size_t> closure, queue;
  for (size_t i = 0; i < candidates.size(); ++i) {
    if (!held[i] || release[i]) continue;
    // ahead[v]: counted lanes in which v precedes t; t is ahead of v iff the
    // other lanes_t - ahead[v] counted lanes that committed t reach theta.
    const int lanes_t = CountPreceding(candidates[i].attest, &ahead);
    auto t_ahead_of = [&](uint32_t v) {
      auto it = ahead.find(v);
      return lanes_t - (it == ahead.end() ? 0 : it->second) >= theta_;
    };
    closure.assign(1, i);
    queue.assign(1, i);
    std::unordered_set<size_t> in_closure{i};
    bool eligible = true;
    while (eligible && !queue.empty()) {
      size_t u = queue.back();
      queue.pop_back();
      if (u != i) CountPreceding(candidates[u].attest, &blockers);
      for (const auto& [v, k] : u == i ? ahead : blockers) {
        if (k < theta_ || t_ahead_of(v)) continue;
        if (state_[v] == kPending) {
          eligible = false;  // a member of D(t) is not eligible yet
          break;
        }
        if (state_[v] != kHeld) continue;  // kept by the fast path
        size_t c = held_index.at(v);
        if (in_closure.insert(c).second) {
          closure.push_back(c);
          queue.push_back(c);
        }
      }
    }
    if (!eligible) continue;
    for (size_t c : closure) release[c] = true;
  }

  std::vector<BofEligibleTxn> released;
  for (size_t i = 0; i < candidates.size(); ++i) {
    if (held[i] && !release[i]) {
      state_[txn[i]] = kPending;
      held_.push_back(std::move(candidates[i]));
    } else {
      state_[txn[i]] = kExecuted;
      released.push_back(std::move(candidates[i]));
    }
  }
  return released;
}

BofSettleResult BofOrderer::Settle(const std::vector<BofCommittedCar>& cars,
                                   const std::vector<int>& claimed_lanes) {
  BofSettleResult result;
  std::vector<std::string> ready;
  // Lane j became settled for every (c, t) in `entries`.
  auto settle = [&](std::multimap<int64_t, std::string>::iterator first,
                    std::multimap<int64_t, std::string>::iterator last) {
    for (auto it = first; it != last; ++it) {
      auto st = txns_.find(it->second);
      if (st != txns_.end() && --st->second.waiting == 0) ready.push_back(it->second);
    }
  };

  // A lane named by a committed claim contributes no ordering report
  // thereafter, including through the Cars committed by this same block.
  for (int j : claimed_lanes) {
    if (discarded_[j]) continue;
    discarded_[j] = true;
    settle(unsettled_[j].begin(), unsettled_[j].end());
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
    auto upto = unsettled_[r].upper_bound(car.index);
    settle(unsettled_[r].begin(), upto);
    unsettled_[r].erase(unsettled_[r].begin(), upto);
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
    TxnState& t = txns_[id];
    for (int j = 1; j <= n_; ++j) {
      if (Settled(t, j)) continue;
      ++t.waiting;
      unsettled_[j].emplace(t.hat[j], id);
      result.armed.emplace_back(j, t.hat[j]);
    }
    if (t.waiting == 0) ready.push_back(id);
  }

  // Eligible: c_hat fixed and every lane settled.  The edge rule counts
  // only lanes not discarded as of this block.
  for (const std::string& id : ready) {
    auto st = txns_.find(id);
    if (st == txns_.end()) continue;
    BofEligibleTxn e;
    e.id = id;
    e.attest = std::move(st->second.attest);
    for (int r = 1; r <= n_; ++r) {
      if (discarded_[r]) e.attest[r] = kNoPos;
    }
    result.eligible.push_back(std::move(e));
    executed_.insert(id);
    txns_.erase(st);
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
  const int words = (m + 63) / 64;

  // rank[r][i]: position of transaction i in lane r's committed order, or -1
  // if lane r holds no (counted) attestation for it.
  std::vector<std::vector<int32_t>> rank(n + 1, std::vector<int32_t>(m, -1));
  {
    std::vector<std::pair<std::pair<int64_t, int32_t>, int>> order;
    order.reserve(m);
    for (int r = 1; r <= n; ++r) {
      order.clear();
      for (int i = 0; i < m; ++i) {
        if (txns[i].attest[r].first >= 0) order.emplace_back(txns[i].attest[r], i);
      }
      std::sort(order.begin(), order.end());
      for (size_t k = 0; k < order.size(); ++k) rank[r][order[k].second] = static_cast<int32_t>(k);
    }
  }

  // Dependency graph (Alg. 3, edge rule Eq. 2) as bitset rows: fwd[a] holds
  // a's successors, bwd[a] its predecessors.  For a fixed a, the counts for
  // every b > a are accumulated lane by lane over contiguous arrays.
  std::vector<uint64_t> fwd(static_cast<size_t>(m) * words, 0);
  std::vector<uint64_t> bwd(static_cast<size_t>(m) * words, 0);
  auto set_edge = [&](int from, int to) {
    fwd[static_cast<size_t>(from) * words + to / 64] |= 1ULL << (to % 64);
    bwd[static_cast<size_t>(to) * words + from / 64] |= 1ULL << (from % 64);
  };
  std::vector<int32_t> both(m), ab(m);
  for (int a = 0; a < m; ++a) {
    std::fill(both.begin() + a + 1, both.end(), 0);
    std::fill(ab.begin() + a + 1, ab.end(), 0);
    for (int r = 1; r <= n; ++r) {
      const int32_t ra = rank[r][a];
      if (ra < 0) continue;
      const int32_t* rr = rank[r].data();
      for (int b = a + 1; b < m; ++b) {
        const int32_t v = rr[b];
        both[b] += v >= 0;
        ab[b] += v > ra;  // lane r saw a before b
      }
    }
    for (int b = a + 1; b < m; ++b) {
      if (ab[b] >= theta) {
        set_edge(a, b);
      } else if (both[b] - ab[b] >= theta) {
        set_edge(b, a);
      }
    }
  }

  // Strongly connected components (Kosaraju over the bitsets: a node's
  // unvisited neighbours are row & unvisited, so each DFS step costs
  // O(m / 64)).  The components are the SCCs of Alg. 3.
  std::vector<uint64_t> unvisited(words, 0);
  for (int i = 0; i < m; ++i) unvisited[i / 64] |= 1ULL << (i % 64);
  auto take_next = [&](const std::vector<uint64_t>& rows, int v, int& cursor) {
    const uint64_t* row = rows.data() + static_cast<size_t>(v) * words;
    for (; cursor < words; ++cursor) {
      uint64_t bits = row[cursor] & unvisited[cursor];
      if (bits != 0) {
        int w = cursor * 64 + __builtin_ctzll(bits);
        unvisited[w / 64] &= ~(1ULL << (w % 64));
        return w;
      }
    }
    return -1;
  };
  std::vector<int> finish;
  finish.reserve(m);
  std::vector<std::pair<int, int>> stack;  // (node, word cursor)
  for (int root = 0; root < m; ++root) {
    if (!(unvisited[root / 64] >> (root % 64) & 1)) continue;
    unvisited[root / 64] &= ~(1ULL << (root % 64));
    stack.emplace_back(root, 0);
    while (!stack.empty()) {
      int w = take_next(fwd, stack.back().first, stack.back().second);
      if (w >= 0) {
        stack.emplace_back(w, 0);
      } else {
        finish.push_back(stack.back().first);
        stack.pop_back();
      }
    }
  }
  std::vector<int> comp(m, -1);
  int num_comp = 0;
  for (int i = 0; i < m; ++i) unvisited[i / 64] |= 1ULL << (i % 64);
  for (auto it = finish.rbegin(); it != finish.rend(); ++it) {
    int root = *it;
    if (!(unvisited[root / 64] >> (root % 64) & 1)) continue;
    unvisited[root / 64] &= ~(1ULL << (root % 64));
    comp[root] = num_comp;
    stack.emplace_back(root, 0);
    while (!stack.empty()) {
      int w = take_next(bwd, stack.back().first, stack.back().second);
      if (w >= 0) {
        comp[w] = num_comp;
        stack.emplace_back(w, 0);
      } else {
        stack.pop_back();
      }
    }
    ++num_comp;
  }

  // Condensation DAG; txns are sorted, so the first member of a component is
  // its lexicographically smallest digest.
  std::vector<std::vector<std::string>> members(num_comp);
  std::vector<std::vector<int>> member_index(num_comp);
  std::vector<int> smallest(num_comp, -1);
  for (int i = 0; i < m; ++i) {
    members[comp[i]].push_back(std::move(txns[i].id));
    member_index[comp[i]].push_back(i);
    if (smallest[comp[i]] == -1) smallest[comp[i]] = i;
  }
  std::vector<std::vector<int>> cadj(num_comp);
  std::vector<int> indeg(num_comp, 0);
  std::vector<int> seen(num_comp, -1);
  std::vector<uint64_t> succ(words);
  for (int c = 0; c < num_comp; ++c) {
    // Union of the members' successor rows.
    std::fill(succ.begin(), succ.end(), 0);
    for (int i : member_index[c]) {
      const uint64_t* row = fwd.data() + static_cast<size_t>(i) * words;
      for (int k = 0; k < words; ++k) succ[k] |= row[k];
    }
    for (int k = 0; k < words; ++k) {
      for (uint64_t bits = succ[k]; bits != 0; bits &= bits - 1) {
        int d = comp[k * 64 + __builtin_ctzll(bits)];
        if (d == c || seen[d] == c) continue;
        seen[d] = c;
        cadj[c].push_back(d);
        ++indeg[d];
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
    targets[j] = unsettled_[j].begin()->first;
  }
  return targets;
}

}  // namespace autobahn
}  // namespace resdb
