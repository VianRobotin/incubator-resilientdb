/* tee_host_check.cpp — standalone checks of the enclave's protocol rules.
 *
 *   make check      (runs against tee_enclave.signed.so in SIM mode)
 *
 * Simulates a committee of n=3 (f=1) by loading one enclave per replica
 * with the same group seed.
 */

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include "platform/consensus/ordering/autobahn/tee/tee_host.h"

using resdb::autobahn::TeeHost;

namespace {

int g_failures = 0;
#define CHECK(cond)                                                   \
  do {                                                                \
    if (!(cond)) {                                                    \
      fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
      g_failures++;                                                   \
    }                                                                 \
  } while (0)

const char* kEnclave = "tee_enclave.signed.so";
const std::string kSeed(32, '\x5a');

std::string Id(int i) {
  return TeeHost::Sha256("txn-" + std::to_string(i));
}

std::vector<std::unique_ptr<TeeHost>> Committee(int n, int f, bool bof) {
  std::vector<std::unique_ptr<TeeHost>> tees(n + 1);
  for (int r = 1; r <= n; r++) {
    tees[r] = std::make_unique<TeeHost>();
    if (!tees[r]->Initialize(kEnclave, r, n, f, bof, kSeed)) {
      fprintf(stderr, "enclave init failed for replica %d\n", r);
      exit(2);
    }
  }
  return tees;
}

void CheckOl() {
  const int n = 3, f = 1;
  auto tees = Committee(n, f, /*bof=*/false);

  // Every replica attests t1 then t2; duplicates are refused (set H).
  std::vector<std::vector<TeeHost::SignedIndicator>> ts(3);
  for (int r = 1; r <= n; r++) {
    for (int t = 1; t <= 2; t++) {
      TeeHost::SignedIndicator s;
      s.signer = r;
      CHECK(tees[r]->OlAttest(Id(t), &s.value, &s.tag));
      ts[t].push_back(s);
    }
    int64_t v;
    std::string tag;
    CHECK(!tees[r]->OlAttest(Id(1), &v, &tag));
  }

  // TEE.Verify accepts genuine timestamps and rejects altered ones.
  std::vector<TeeHost::VerifyItem> items = {
      {TeeHost::OlTimestampMsg(1, Id(1), ts[1][0].value), ts[1][0].tag},
      {TeeHost::OlTimestampMsg(1, Id(1), ts[1][0].value + 1), ts[1][0].tag},
      {TeeHost::OlTimestampMsg(2, Id(1), ts[1][0].value), ts[1][0].tag}};
  auto ok = tees[2]->VerifyBatch(items);
  CHECK(ok[0] && !ok[1] && !ok[2]);

  // Local key = (f+1)-th smallest of the genuine timestamps; f+1 are needed.
  int64_t key = 0;
  CHECK(!tees[1]->OlLocalKey(Id(1), {ts[1][0]}, &key));
  CHECK(tees[1]->OlLocalKey(Id(1), ts[1], &key));
  std::vector<int64_t> v = {ts[1][0].value, ts[1][1].value, ts[1][2].value};
  std::sort(v.begin(), v.end());
  CHECK(key == v[f]);
  // A forged record is ignored, so it cannot lower the key.
  auto forged = ts[2];
  forged[0].value = 1;
  int64_t key2 = 0;
  CHECK(tees[1]->OlLocalKey(Id(2), forged, &key2));
  CHECK(key2 != 1);

  // Sealing: the frontier stays below t2 until t2's key is sealed.
  TeeHost::SealedCar car1;
  CHECK(!tees[1]->OlSealCar({{Id(1), key + 1}}, &car1));  // wrong key refused
  CHECK(tees[1]->OlSealCar({{Id(1), key}}, &car1));
  CHECK(car1.index == 1);
  CHECK(car1.frontier == ts[2][0].value - 1);
  CHECK(car1.hash == TeeHost::OlCarHash(1, 1, std::string(32, '\0'),
                                        {{Id(1), key}}, car1.frontier));
  ok = tees[3]->VerifyBatch({{TeeHost::CarSigMsg(1, car1.hash), car1.tag}});
  CHECK(ok[0]);

  TeeHost::SealedCar car2;
  CHECK(tees[1]->OlSealCar({{Id(2), key2}}, &car2));
  CHECK(car2.index == 2);
  CHECK(car2.frontier == ts[2][0].value);
  // Heartbeat: a fresh indicator lets an empty Car advance the frontier.
  std::string hb;
  int64_t hb_ts = 0;
  CHECK(tees[1]->Heartbeat(&hb, &hb_ts));
  TeeHost::SealedCar car3;
  CHECK(tees[1]->OlSealCar({}, &car3));
  CHECK(car3.frontier == hb_ts);
  CHECK(car3.hash == TeeHost::OlCarHash(1, 3, car2.hash, {}, hb_ts));
  // Timestamps the host feeds the enclave strictly increase.
  CHECK(hb_ts > ts[2][0].value);

  // H is never pruned: a transaction is attested at most once.
  int64_t again = 0;
  std::string again_tag;
  CHECK(!tees[1]->OlAttest(Id(1), &again, &again_tag));
}

void CheckBof() {
  const int n = 3, f = 1;
  auto tees = Committee(n, f, /*bof=*/true);

  // Replica r places t1, t2 in its open Car 1.
  std::vector<std::vector<TeeHost::SignedIndicator>> pl(3);
  for (int r = 1; r <= n; r++) {
    for (int t = 1; t <= 2; t++) {
      TeeHost::SignedIndicator s;
      s.signer = r;
      CHECK(tees[r]->BofPlace(Id(t), &s.value, &s.tag));
      CHECK(s.value == 1);
      pl[t].push_back(s);
    }
  }
  // Seal Car 1 before the windows close: w stays 0.
  TeeHost::SealedCar c1;
  CHECK(tees[1]->BofSealCar(&c1));
  CHECK(c1.index == 1 && c1.frontier == 0);
  CHECK(c1.bof_items.size() == 2 && c1.bof_items[0].id == Id(1) &&
        c1.bof_items[1].id == Id(2));
  CHECK(c1.hash == TeeHost::BofCarHash(1, 1, std::string(32, '\0'),
                                       c1.bof_items, 0, c1.m, n));

  // Close t1's window only: w still 0.  Then t2: w reaches 1 at next seal.
  CHECK(tees[1]->BofRecordAnnouncements(Id(1), pl[1]));
  CHECK(!tees[1]->BofRecordAnnouncements(Id(1), pl[1]));  // window already closed
  std::string hb;
  int64_t hb_car = 0;
  CHECK(tees[1]->Heartbeat(&hb, &hb_car));
  CHECK(hb_car == 2);
  TeeHost::SealedCar c2;
  CHECK(tees[1]->BofSealCar(&c2));
  CHECK(c2.frontier == 0);
  CHECK(c2.bof_items.size() == 1 && c2.bof_items[0].heartbeat);

  // Forge replica 3's announcement for t2 to a larger Car: it is ignored.
  auto anns = pl[2];
  anns[2].value = 7;
  CHECK(tees[1]->BofRecordAnnouncements(Id(2), anns));
  TeeHost::SealedCar c3;
  CHECK(tees[1]->Heartbeat(&hb, &hb_car));
  CHECK(tees[1]->BofSealCar(&c3));
  // Car 1 (t1,t2) and Car 2 (heartbeat) are fully closed; Car 3 holds only a
  // heartbeat, so w covers it too.
  CHECK(c3.frontier == 3);
  CHECK(c3.m[1].present && c3.m[1].car == 1);
  CHECK(c3.m[2].present && c3.m[2].car == 1);
  // Replica 3's genuine announcement for t1 (Car 1) counts; the forged
  // Car-7 announcement for t2 does not.
  CHECK(c3.m[3].present && c3.m[3].car == 1);
  auto ok = tees[2]->VerifyBatch(
      {{TeeHost::BofPlacementMsg(2, c3.m[2].id, c3.m[2].car), c3.m[2].tag},
       {TeeHost::CarSigMsg(1, c3.hash), c3.tag}});
  CHECK(ok[0] && ok[1]);

  // H is never pruned: a transaction is placed at most once.
  int64_t c = 0;
  std::string ptag;
  CHECK(!tees[1]->BofPlace(Id(1), &c, &ptag));

  // Block signing refuses equivocation at the same height.
  std::string tag;
  CHECK(tees[1]->SignBlock(1, 1, TeeHost::Sha256("a"), &tag));
  CHECK(!tees[1]->SignBlock(1, 1, TeeHost::Sha256("b"), &tag));
  CHECK(tees[1]->SignBlock(1, 2, TeeHost::Sha256("c"), &tag));
  CHECK(!tees[1]->SignBlock(0, 5, TeeHost::Sha256("d"), &tag));
  ok = tees[3]->VerifyBatch(
      {{TeeHost::BlockMsg(1, 1, 2, TeeHost::Sha256("c")), tag}});
  CHECK(ok[0]);
}

}  // namespace

int main(int argc, char** argv) {
  if (argc > 1) kEnclave = argv[1];
  CheckOl();
  CheckBof();
  if (g_failures == 0) printf("tee_host_check: all checks passed\n");
  return g_failures == 0 ? 0 : 1;
}
