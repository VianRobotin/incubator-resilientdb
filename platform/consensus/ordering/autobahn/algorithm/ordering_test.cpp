#include <gtest/gtest.h>

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

}  // namespace
}  // namespace autobahn
}  // namespace resdb
