#pragma once
/* tee_host.h — untrusted host wrapper for the Pearl enclave.
 *
 * Deliberately avoids including <sgx_urts.h> so that translation units that
 * only need the declarations do not depend on the SGX SDK headers.  The SGX
 * SDK is only required by tee_host.cpp.
 *
 * The static helpers rebuild, from received fields, the exact bytes the
 * enclave authenticates (tee_msg.h), so a verifier can hand them to
 * VerifyBatch (TEE.Verify) and recompute Car hashes.
 */

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace resdb {
namespace autobahn {

class TeeHost {
 public:
  TeeHost() = default;
  ~TeeHost();

  /* Load the signed enclave and initialise it for replica `self_id` of `n`.
   * `group_seed` (32 bytes) provisions the committee authentication key. */
  bool Initialize(const std::string& enclave_path, int self_id, int n, int f,
                  bool bof, const std::string& group_seed);
  bool IsOk() const { return ok_; }

  /* A signed indicator received from replica `signer`: an OL timestamp T or
   * a BOF placement announcement c, with its 32-byte tag. */
  struct SignedIndicator {
    int signer = 0;
    int64_t value = 0;
    std::string tag;
  };

  struct OlItem {
    std::string id;
    int64_t key = 0;
  };
  struct BofItem {
    std::string id;
    bool heartbeat = false;
  };
  /* One entry M_r[j] of a reception frontier. */
  struct Announcement {
    bool present = false;
    int64_t car = 0;
    std::string id;
    std::string tag;
  };
  struct SealedCar {
    int64_t index = 0;
    int64_t frontier = 0;  // G_r (OL) or w_r (BOF)
    std::string hash;
    std::string tag;
    std::vector<BofItem> bof_items;    // BOF only: the enclave's order
    std::vector<Announcement> m;       // BOF only: M_r[1..n] (index 0 unused)
  };

  /* ---- Batched per-transaction calls (the dissemination hot path) ---- */
  static const int kTagLen = 16;

  /* OnFirstReceive for `count` transactions whose 32-byte ids are packed in
   * `ids`.  OL: attests each with an increasing host timestamp; BOF: places
   * each in the open Car.  values[i] = T or c_r(t), tags = count x kTagLen
   * bytes, ok[i] = 0 if t_i was already attested. */
  bool AttestBatch(const uint8_t* ids, uint32_t count,
                   std::vector<int64_t>* values, std::string* tags,
                   std::vector<uint8_t>* ok);

  /* AfterCollectionWindow for `count` transactions.  `collected` holds, per
   * transaction, AppendCollected(id, n) followed by n AppendRecord(...).
   * OL: keys[i] = K_r(t_i); BOF: records the announcements.  ok[i] = 0 if
   * the enclave refused (OL: fewer than f+1 genuine timestamps). */
  bool CloseWindowsBatch(const std::string& collected, uint32_t count,
                         std::vector<int64_t>* keys, std::vector<uint8_t>* ok);
  static void AppendCollected(std::string* buf, const uint8_t* id,
                              uint32_t num_records);
  static void AppendRecord(std::string* buf, int signer, int64_t value,
                           const char* tag);

  /* ---- Pearl-OL ---- */
  /* Feeds the enclave the host's clock reading (bumped past the previous
   * one) and returns it in *ts.  Returns false if t was already attested. */
  bool OlAttest(const std::string& id, int64_t* ts, std::string* tag);
  bool OlLocalKey(const std::string& id,
                  const std::vector<SignedIndicator>& timestamps,
                  int64_t* key);
  bool OlSealCar(const std::vector<OlItem>& items, SealedCar* out);

  /* ---- Pearl-BOF ---- */
  /* Returns false if t was already attested. */
  bool BofPlace(const std::string& id, int64_t* car, std::string* tag);
  bool BofRecordAnnouncements(const std::string& id,
                              const std::vector<SignedIndicator>& placements);
  bool BofSealCar(SealedCar* out);

  /* Heartbeat: OL returns the new timestamp in *value; BOF returns the no-op
   * item's identifier in *id and its Car index in *value. */
  bool Heartbeat(std::string* id, int64_t* value);

  /* Enclave garbage collection: drop executed transactions from H. */
  bool Forget(const std::vector<std::string>& ids);

  /* TEE.SignBlock; refuses a second block at the same (view, height). */
  bool SignBlock(int64_t view, int64_t height, const std::string& digest,
                 std::string* tag);

  /* TEE.Verify over a batch of (message, tag) pairs. */
  struct VerifyItem {
    std::string msg;
    std::string tag;
  };
  std::vector<bool> VerifyBatch(const std::vector<VerifyItem>& items);

  /* ---- Message builders (mirror tee_msg.h) ---- */
  static std::string OlTimestampMsg(int signer, const std::string& id,
                                    int64_t ts);
  static std::string BofPlacementMsg(int signer, const std::string& id,
                                     int64_t car);
  static std::string CarSigMsg(int lane, const std::string& car_hash);
  static std::string BlockMsg(int signer, int64_t view, int64_t height,
                              const std::string& digest);

  static std::string OlCarHash(int lane, int64_t index,
                               const std::string& prev_hash,
                               const std::vector<OlItem>& items,
                               int64_t frontier);
  static std::string BofCarHash(int lane, int64_t index,
                                const std::string& prev_hash,
                                const std::vector<BofItem>& items, int64_t w,
                                const std::vector<Announcement>& m, int n);

  static std::string Sha256(const std::string& data);

 private:
  uint64_t eid_ = 0; /* sgx_enclave_id_t is typedef uint64_t */
  bool ok_ = false;
  int n_ = 0;
  bool bof_ = false;
  int64_t last_ts_ = 0;  /* OL: last timestamp fed to the enclave */
  std::mutex mu_;
};

}  // namespace autobahn
}  // namespace resdb
