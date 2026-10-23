#pragma once

#include <cstdint>
#include <deque>
#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace resdb {
namespace autobahn {

// A Car of Pearl-BOF as committed by consensus.  `items` are in the order the
// lane's enclave observed them (heartbeats included, flagged); `w` and `m`
// form the reception frontier (w_r, M_r) sealed into the Car, with m[j] the
// largest placement announcement collected from replica j (index 0 unused).
struct BofCommittedCar {
  struct Announcement {
    bool present = false;
    int64_t car = 0;
  };
  int lane = 0;
  int64_t index = 0;
  std::vector<std::string> items;
  std::vector<bool> heartbeat;
  int64_t w = 0;
  std::vector<Announcement> m;
};

struct BofCommitResult {
  // Batches to execute, in order; transactions inside a batch sorted by digest.
  std::vector<std::vector<std::string>> batches;
  // (lane j, c_hat_j(t)) pairs fixed by this block for lanes not yet settled:
  // each arms a delinquency timer (Sec. 7.1).
  std::vector<std::pair<int, int64_t>> armed;
};

// Pearl-BOF ordering rule (paper Sec. 7), evaluated on committed Cars only.
//
// Settlement (Sec. 7.1): lane r covers t once its committed prefix contains t
// and one of its committed Cars carries w_r >= c_r(t); M_r^t is the
// announcement vector of the first such Car.  With C_{f+1}(t) the first f+1
// covering lanes (in commitment order), c_hat_j(t) = max_{r in C} M_r^t[j].
// Lane j is settled for t once its committed prefix reaches c_hat_j(t) or a
// committed delinquency claim names j.  t is eligible once every lane is
// settled.
//
// Ordering (Sec. 7.2-7.3, Alg. 3): over the eligible transactions T, for each
// pair, R = non-discarded lanes whose committed attestations contain both;
// add t_a -> t_b iff |{R : s_R(t_a) < s_R(t_b)}| >= theta = ceil(gamma(n-f)),
// else t_b -> t_a iff the complementary count reaches theta.  SCCs become
// batches, emitted in a topological order of the condensation with ties
// broken by the smallest digest of each SCC.
class BofOrderer {
 public:
  BofOrderer(int n, int f, double gamma);

  // Processes one committed block: its delinquency claims (`claimed_lanes`)
  // and its newly committed Cars, in commitment order.
  BofCommitResult Commit(const std::vector<BofCommittedCar>& cars,
                         const std::vector<int>& claimed_lanes);

  // For every non-discarded lane, the lowest c_hat_j(t) of a pending
  // transaction that the lane's committed prefix has not reached: the
  // position a delinquency claim <j, c> would name.
  std::map<int, int64_t> ClaimTargets() const;

  bool Discarded(int lane) const { return discarded_[lane]; }
  int64_t CommittedIndex(int lane) const { return committed_[lane]; }
  int Theta() const { return theta_; }
  size_t PendingSize() const { return txns_.size(); }

 private:
  struct Pos {
    int64_t car = -1;
    int32_t pos = -1;
    bool valid() const { return car >= 0; }
    bool operator<(const Pos& o) const {
      return car != o.car ? car < o.car : pos < o.pos;
    }
  };
  struct TxnState {
    std::vector<Pos> attest;        // per lane; committed position s_R(t)
    std::vector<int> coverers;      // first f+1 covering lanes
    std::vector<int64_t> hat;       // running max of M_r^t, then c_hat_j(t)
    bool hat_fixed = false;
  };

  bool Settled(const TxnState& t, int j) const;
  std::vector<std::vector<std::string>> Order(std::vector<std::string> txns);

  int n_, f_;
  int theta_;
  std::vector<int64_t> committed_;  // per lane: committed prefix length
  std::vector<bool> discarded_;
  // Per lane r: (Car index, id) of committed items not yet covered by r.
  std::vector<std::deque<std::pair<int64_t, std::string>>> uncovered_;
  std::unordered_map<std::string, TxnState> txns_;
  std::set<std::string> hat_fixed_;          // c_hat fixed, not yet eligible
  std::vector<std::multiset<int64_t>> unsettled_;  // per lane: open c_hat
  std::unordered_set<std::string> executed_;
};

}  // namespace autobahn
}  // namespace resdb
