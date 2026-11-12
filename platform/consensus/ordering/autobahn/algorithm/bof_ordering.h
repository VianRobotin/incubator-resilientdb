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

// An eligible transaction with the committed attestations the edge rule
// counts: attest[r] = (Car index, position in Car) of lane r's attestation,
// or (-1, -1) if the lane has none or is discarded (index 0 unused).
struct BofEligibleTxn {
  std::string id;
  std::vector<std::pair<int64_t, int32_t>> attest;
};

struct BofSettleResult {
  std::vector<BofEligibleTxn> eligible;
  // (lane j, c_hat_j(t)) pairs fixed by this block for lanes not yet settled:
  // each arms a delinquency timer (Sec. 7.1).
  std::vector<std::pair<int, int64_t>> armed;
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
// Release across execution steps (Sec. 7.1): a lane is counted if no
// committed claim names it; v blocks an eligible u if v precedes u in at
// least theta counted lanes; t is ahead of v if at least theta counted lanes
// committed t with v not before it.  The closure D(t) of an eligible t is
// the smallest set that contains t and, for every eligible u in D(t), each
// unexecuted blocker v of u that t is not ahead of.  Once every transaction
// of D(t) is eligible, its unexecuted ones are released, and the step orders
// the released transactions.  A blocker left out of D(t) lies on a cycle
// with the member it blocks (Lemma 6), i.e. in the same batch, which
// Definition 2 lets the protocol output over several steps.  Stopping at
// the transactions t is ahead of bounds D(t) under continuous load, where
// requiring every blocker to be released in turn could chain forever.
//
// It only affects execution order, so it runs on the execution thread,
// apart from settlement (which arms the delinquency timers).  Feed it every
// committed block in commit order: OnCommitted, then Release.
class BofHoldBack {
 public:
  BofHoldBack(int n, int theta);
  void OnCommitted(const std::vector<BofCommittedCar>& cars,
                   const std::vector<int>& claimed_lanes);
  // The transactions the block made eligible; returns those that may
  // execute now (the others are kept and reconsidered with later blocks).
  std::vector<BofEligibleTxn> Release(std::vector<BofEligibleTxn> eligible);
  size_t HeldSize() const { return held_.size(); }
  // Cost breakdown of the last Release (diagnostics).
  struct ReleaseStats {
    size_t candidates = 0, fast_held = 0, blockers_calls = 0, queued = 0;
    int64_t fast_us = 0, closure_us = 0, compact_us = 0, fast_rounds = 0;
    int64_t fast_scanned = 0, closure_scanned = 0;
    int64_t roots = 0, failed_roots = 0, visits = 0, ahead_calls = 0;
    std::string ToString() const;
  };
  const ReleaseStats& LastStats() const { return stats_; }
  // Entries kept in the lane queues (executed ones included until dropped).
  size_t QueuedSize() const {
    size_t total = 0;
    for (const auto& q : lane_) total += q.size();
    return total;
  }

 private:
  using Pos = std::pair<int64_t, int32_t>;
  // kCandidate: eligible this step; kHeld: eligible but held back by the
  // fast path of Release (then decided by its closure).
  enum State : uint8_t { kPending, kCandidate, kHeld, kExecuted };
  struct Entry {
    Pos pos;
    uint32_t txn;
  };
  uint32_t Intern(const std::string& id);
  // First pending or held entry of a lane (dropping executed ones from the
  // front); nullptr if none.
  const Entry* FirstPending(int lane);
  // Appends to `out` every pending or held transaction that precedes the
  // attestations `attest` in at least theta counted lanes: the blockers that
  // can keep a closure from release (the candidates the fast path keeps are
  // released, and their own blockers with them).  Scans `blocking_`.
  void Blockers(const std::vector<Pos>& attest, std::vector<uint32_t>* out);
  // Drops executed entries from lanes that have grown enough since the last
  // compaction.
  void Compact();

  int n_, theta_;
  std::vector<bool> discarded_;
  // Per non-discarded lane: its committed transactions in lane order;
  // executed ones are skipped lazily.
  std::vector<std::deque<Entry>> lane_;
  std::vector<size_t> compacted_;  // per lane: size after the last compaction
  std::unordered_map<std::string, uint32_t> index_;
  std::vector<State> state_;
  // Per unexecuted transaction: its committed position in each lane.
  std::unordered_map<uint32_t, std::vector<Pos>> where_;
  // Per-transaction counters for the scans (all zero between scans).
  std::vector<int> count_;
  std::vector<uint32_t> touched_;
  // Per lane, during Release: its pending and held entries, in lane order.
  std::vector<std::vector<Entry>> blocking_;
  std::vector<BofEligibleTxn> held_;
  ReleaseStats stats_;
};

class BofOrderer {
 public:
  BofOrderer(int n, int f, double gamma);

  // Settlement of one committed block: its delinquency claims
  // (`claimed_lanes`) and its newly committed Cars, in commitment order.
  // Cheap; run at commit so that delinquency timers are armed then.
  BofSettleResult Settle(const std::vector<BofCommittedCar>& cars,
                         const std::vector<int>& claimed_lanes);

  // Alg. 3 over the transactions a Settle() call made eligible: a pure
  // function of its input, so it can run on another thread.
  static std::vector<std::vector<std::string>> Order(
      std::vector<BofEligibleTxn> txns, int n, int theta);

  // Settle(), the release rule and Order() (tests).
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
  using Pos = std::pair<int64_t, int32_t>;  // (Car index, position in Car)
  static constexpr Pos kNoPos{-1, -1};
  struct TxnState {
    std::vector<Pos> attest;        // per lane; committed position s_R(t)
    std::vector<int> coverers;      // first f+1 covering lanes
    std::vector<int64_t> hat;       // running max of M_r^t, then c_hat_j(t)
    bool hat_fixed = false;
    int waiting = 0;                // once fixed: lanes not yet settled
  };

  bool Settled(const TxnState& t, int j) const;

  int n_, f_;
  int theta_;
  std::vector<int64_t> committed_;  // per lane: committed prefix length
  std::vector<bool> discarded_;
  // Per lane r: (Car index, id) of committed items not yet covered by r.
  std::vector<std::deque<std::pair<int64_t, std::string>>> uncovered_;
  std::unordered_map<std::string, TxnState> txns_;
  // Per lane j: (c_hat_j(t), t) for fixed transactions lane j has not
  // settled yet.  Settlement only visits the lanes that advance.
  std::vector<std::multimap<int64_t, std::string>> unsettled_;
  std::unordered_set<std::string> executed_;
  BofHoldBack hold_;  // for Commit()
};

}  // namespace autobahn
}  // namespace resdb
