#include <gtest/gtest.h>
#include <set>
#include <queue>

#include <algorithm>
#include <random>
#include <numeric>

#include "platform/consensus/ordering/autobahn/algorithm/bof_ordering.h"
#include "platform/consensus/ordering/autobahn/algorithm/ol_ordering.h"

namespace resdb {
namespace autobahn {
namespace {

using Batches = std::vector<std::vector<std::string>>;

OlCommittedCar OlCar(int lane, int64_t index, int64_t frontier,
                     std::vector<std::pair<std::string, int64_t>> items = {}) {
  OlCommittedCar car;
  car.lane = lane;
  car.index = index;
  car.frontier = frontier;
  car.items = std::move(items);
  return car;
}

// n = 3, f = 1: K(t) is fixed by the first 2 committed keys.
TEST(OlOrdererTest, FinalKeyIsMinOfFirstFPlusOneKeys) {
  OlOrderer ol(3, 1);
  auto out = ol.Commit({OlCar(1, 1, 500, {{"t", 120}}),
                        OlCar(2, 1, 500, {{"t", 100}}),
                        OlCar(3, 1, 500, {{"t", 50}})});  // third key ignored
  EXPECT_EQ(ol.Threshold(), 500);
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0], "t");
}

TEST(OlOrdererTest, ExecutionIsStrictlyBelowThreshold) {
  OlOrderer ol(3, 1);
  auto out = ol.Commit({OlCar(1, 1, 200, {{"t", 200}}),
                        OlCar(2, 1, 200, {{"t", 200}}), OlCar(3, 1, 200)});
  EXPECT_EQ(ol.Threshold(), 200);
  EXPECT_TRUE(out.empty());  // K(t) == tau is not eligible
  out = ol.Commit({OlCar(1, 2, 201), OlCar(2, 2, 201)});
  ASSERT_EQ(out.size(), 1u);
}

// An unfixed transaction bounds tau below G^(f+1): the replica that published
// its key in Car i contributes G^(i-1), not its current frontier.
TEST(OlOrdererTest, UnfixedTransactionBoundsThreshold) {
  OlOrderer ol(3, 1);
  auto out = ol.Commit({
      OlCar(1, 1, 100, {{"t0", 90}}),
      OlCar(2, 1, 300, {{"t0", 95}, {"t1", 200}}),
      OlCar(3, 1, 150),
      OlCar(3, 2, 300, {{"t1", 210}, {"u", 400}}),
  });
  // G = {100, 300, 300} -> G^(2) = 300.  "u" has one key, published by lane
  // 3 in Car 2: K_(u) = 2nd smallest of {G_1=100, G_2=300, G_3^(1)=150}.
  EXPECT_EQ(ol.Threshold(), 150);
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0], "t0");  // K = 90 < 150; t1 (K = 200) must wait
  out = ol.Commit({OlCar(1, 2, 300, {{"u", 410}})});  // u fixed, K = 400
  EXPECT_EQ(ol.Threshold(), 300);
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0], "t1");
}

TEST(OlOrdererTest, TiesBrokenByDigest) {
  OlOrderer ol(3, 1);
  auto out = ol.Commit({OlCar(1, 1, 500, {{"b", 10}, {"a", 10}}),
                        OlCar(2, 1, 500, {{"a", 10}, {"b", 10}}),
                        OlCar(3, 1, 500)});
  ASSERT_EQ(out.size(), 2u);
  EXPECT_EQ(out[0], "a");
  EXPECT_EQ(out[1], "b");
}

BofCommittedCar BofCar(int n, int lane, int64_t index,
                       std::vector<std::string> items, int64_t w,
                       std::vector<int64_t> m /* m[j-1], 0 = absent */) {
  BofCommittedCar car;
  car.lane = lane;
  car.index = index;
  car.items = std::move(items);
  car.heartbeat.assign(car.items.size(), false);
  car.w = w;
  car.m.assign(n + 1, BofCommittedCar::Announcement());
  for (int j = 1; j <= n; ++j) {
    if (m[j - 1] > 0) {
      car.m[j].present = true;
      car.m[j].car = m[j - 1];
    }
  }
  return car;
}

BofCommittedCar Heartbeat(int n, int lane, int64_t index, int64_t w,
                          std::vector<int64_t> m) {
  BofCommittedCar car = BofCar(n, lane, index, {"hb"}, w, std::move(m));
  car.heartbeat[0] = true;
  return car;
}

TEST(BofOrdererTest, ThetaIsCeilGammaNMinusF) {
  EXPECT_EQ(BofOrderer(3, 1, 1.0).Theta(), 2);
  EXPECT_EQ(BofOrderer(4, 1, 1.0).Theta(), 3);
  EXPECT_EQ(BofOrderer(7, 1, 0.75).Theta(), 5);  // ceil(4.5)
}

// All replicas receive "z" before "a": the edge z -> a decides the order,
// not the digest.
TEST(BofOrdererTest, UnanimousOrderIsRespected) {
  const int n = 3;
  BofOrderer bof(n, 1, 1.0);
  std::vector<BofCommittedCar> cars;
  for (int r = 1; r <= n; ++r) cars.push_back(BofCar(n, r, 1, {"z", "a"}, 0, {0, 0, 0}));
  for (int r = 1; r <= n; ++r) cars.push_back(Heartbeat(n, r, 2, 1, {1, 1, 1}));
  auto res = bof.Commit(cars, {});
  ASSERT_EQ(res.batches, (Batches{{"z"}, {"a"}}));
}

// x -> y -> z -> x, each edge backed by 2 = theta lanes: one batch.
TEST(BofOrdererTest, CondorcetCycleFormsOneBatch) {
  const int n = 3;
  BofOrderer bof(n, 1, 1.0);
  auto res = bof.Commit({BofCar(n, 1, 1, {"x", "y", "z"}, 1, {1, 1, 1}),
                         BofCar(n, 2, 1, {"y", "z", "x"}, 1, {1, 1, 1}),
                         BofCar(n, 3, 1, {"z", "x", "y"}, 1, {1, 1, 1})},
                        {});
  ASSERT_EQ(res.batches, (Batches{{"x", "y", "z"}}));
}

// Lane 3's frontier says lane 2 placed t at Car 2; t waits until lane 2's
// committed prefix reaches Car 2, and a delinquency claim releases it.
TEST(BofOrdererTest, SettlementWaitsForAnnouncedCarOrClaim) {
  const int n = 3;
  BofOrderer bof(n, 1, 1.0);
  auto res = bof.Commit({BofCar(n, 1, 1, {"t"}, 1, {1, 0, 1}),
                         BofCar(n, 3, 1, {"t"}, 1, {1, 2, 1})},
                        {});
  EXPECT_TRUE(res.batches.empty());
  ASSERT_EQ(res.armed.size(), 1u);
  EXPECT_EQ(res.armed[0], std::make_pair(2, int64_t{2}));
  EXPECT_EQ(bof.ClaimTargets(), (std::map<int, int64_t>{{2, 2}}));

  res = bof.Commit({}, /*claimed_lanes=*/{2});
  ASSERT_EQ(res.batches, (Batches{{"t"}}));
  EXPECT_TRUE(bof.Discarded(2));
  EXPECT_TRUE(bof.ClaimTargets().empty());
}

TEST(BofOrdererTest, SettlementByLaneProgress) {
  const int n = 3;
  BofOrderer bof(n, 1, 1.0);
  auto res = bof.Commit({BofCar(n, 1, 1, {"t"}, 1, {1, 2, 1}),
                         BofCar(n, 3, 1, {"t"}, 1, {1, 2, 1}),
                         BofCar(n, 2, 1, {}, 0, {0, 0, 0})},
                        {});
  EXPECT_TRUE(res.batches.empty());
  res = bof.Commit({BofCar(n, 2, 2, {"t"}, 0, {0, 0, 0})}, {});
  ASSERT_EQ(res.batches, (Batches{{"t"}}));
}

// Coverage needs f+1 = 2 lanes whose committed frontier passed t's Car.
TEST(BofOrdererTest, CoverageNeedsFPlusOneLanes) {
  const int n = 3;
  BofOrderer bof(n, 1, 1.0);
  auto res = bof.Commit({BofCar(n, 1, 1, {"t"}, 1, {1, 1, 1}),
                         BofCar(n, 2, 1, {"t"}, 0, {0, 0, 0}),
                         BofCar(n, 3, 1, {"t"}, 0, {0, 0, 0})},
                        {});
  EXPECT_TRUE(res.batches.empty());
  res = bof.Commit({Heartbeat(n, 2, 2, 1, {1, 1, 1})}, {});
  ASSERT_EQ(res.batches, (Batches{{"t"}}));
}


// The straightforward Alg. 3 (adjacency lists, recursive-free Tarjan), kept
// as the reference the optimized BofOrderer::Order must match exactly.
Batches ReferenceOrder(
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


// The optimized ordering must produce exactly the reference batches, on
// random attestations including absent lanes, ties and Condorcet cycles.
TEST(BofOrdererTest, OptimizedOrderMatchesReference) {
  std::mt19937 rng(42);
  for (int trial = 0; trial < 200; ++trial) {
    const int n = 3 + 2 * (trial % 7);          // 3 .. 15
    const int f = (n - 1) / 2;
    const int theta = (trial % 3 == 0) ? f + 1 : n - f;
    const int m = 1 + static_cast<int>(rng() % 300);
    std::vector<BofEligibleTxn> txns(m);
    std::vector<std::vector<int>> perm(n + 1);
    for (int r = 1; r <= n; ++r) {
      perm[r].resize(m);
      std::iota(perm[r].begin(), perm[r].end(), 0);
      // Mostly agreeing lanes with local swaps, so both DAGs and cycles occur.
      for (int k = 0; k < m / 4; ++k) {
        int i = rng() % m, j = std::min(m - 1, i + static_cast<int>(rng() % 4));
        std::swap(perm[r][i], perm[r][j]);
      }
    }
    for (int i = 0; i < m; ++i) {
      txns[i].id = "t" + std::to_string(rng() % 100000) + "_" + std::to_string(i);
      txns[i].attest.assign(n + 1, {-1, -1});
    }
    for (int r = 1; r <= n; ++r) {
      for (int k = 0; k < m; ++k) {
        if (rng() % 10 == 0) continue;  // lane holds no attestation
        int i = perm[r][k];
        txns[i].attest[r] = {k / 50, static_cast<int32_t>(k % 50)};
      }
    }
    EXPECT_EQ(BofOrderer::Order(txns, n, theta), ReferenceOrder(txns, n, theta))
        << "trial " << trial << " n=" << n << " m=" << m;
  }
}

}  // namespace
}  // namespace autobahn
}  // namespace resdb
