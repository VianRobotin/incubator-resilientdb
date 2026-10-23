/* tee_host.cpp — untrusted host wrapper for the Pearl enclave.
 *
 * Compiled by the standalone Makefile into libtee_host.a.
 * No glog dependency: uses fprintf(stderr) for diagnostics.
 */

#include "platform/consensus/ordering/autobahn/tee/tee_host.h"
#include "platform/consensus/ordering/autobahn/tee/gen/tee_enclave_u.h"
#include "platform/consensus/ordering/autobahn/tee/tee_msg.h"

#include <openssl/sha.h>
#include <sgx_urts.h>
#include <sys/time.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <memory>

namespace resdb {
namespace autobahn {

namespace {
int64_t WallClockUs() {
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}
}  // namespace

namespace {

const uint8_t* U8(const std::string& s) {
  return reinterpret_cast<const uint8_t*>(s.data());
}

/* Identifiers are 32-byte SHA-256 digests; pad/truncate defensively. */
std::string Id32(const std::string& id) {
  std::string out(PEARL_ID_LEN, '\0');
  std::memcpy(&out[0], id.data(), std::min(id.size(), out.size()));
  return out;
}

bool Check(const char* what, sgx_status_t ret, sgx_status_t ecall_ret) {
  if (ret != SGX_SUCCESS) {
    fprintf(stderr, "[TEE] %s: SGX error 0x%x\n", what, ret);
    return false;
  }
  return ecall_ret == SGX_SUCCESS;
}

std::string PackIndicators(const std::vector<TeeHost::SignedIndicator>& recs) {
  std::string out(recs.size() * PEARL_SIGNED_TS_REC, '\0');
  uint8_t* p = reinterpret_cast<uint8_t*>(&out[0]);
  for (const auto& r : recs) {
    pearl_put_u32(p, static_cast<uint32_t>(r.signer));
    pearl_put_i64(p + 4, r.value);
    std::memcpy(p + 12, r.tag.data(), std::min<size_t>(r.tag.size(), PEARL_TAG_LEN));
    p += PEARL_SIGNED_TS_REC;
  }
  return out;
}

std::string OlItemBytes(const std::vector<TeeHost::OlItem>& items) {
  std::string out(items.size() * PEARL_OL_ITEM_REC, '\0');
  uint8_t* p = reinterpret_cast<uint8_t*>(&out[0]);
  for (const auto& it : items) {
    std::memcpy(p, Id32(it.id).data(), PEARL_ID_LEN);
    pearl_put_i64(p + PEARL_ID_LEN, it.key);
    p += PEARL_OL_ITEM_REC;
  }
  return out;
}

std::string BofItemBytes(const std::vector<TeeHost::BofItem>& items) {
  std::string out(items.size() * PEARL_BOF_ITEM_REC, '\0');
  uint8_t* p = reinterpret_cast<uint8_t*>(&out[0]);
  for (const auto& it : items) {
    std::memcpy(p, Id32(it.id).data(), PEARL_ID_LEN);
    p[PEARL_ID_LEN] = it.heartbeat ? 1 : 0;
    p += PEARL_BOF_ITEM_REC;
  }
  return out;
}

std::string MBytes(const std::vector<TeeHost::Announcement>& m, int n) {
  std::string out(static_cast<size_t>(n) * PEARL_M_REC, '\0');
  uint8_t* base = reinterpret_cast<uint8_t*>(&out[0]);
  for (int j = 1; j <= n && j < static_cast<int>(m.size()); j++) {
    const auto& a = m[j];
    if (!a.present) continue;
    uint8_t* rec = base + (j - 1) * PEARL_M_REC;
    rec[0] = 1;
    pearl_put_i64(rec + 1, a.car);
    std::memcpy(rec + 9, Id32(a.id).data(), PEARL_ID_LEN);
    std::memcpy(rec + 9 + PEARL_ID_LEN, a.tag.data(),
                std::min<size_t>(a.tag.size(), PEARL_TAG_LEN));
  }
  return out;
}

std::string CarHash(uint8_t domain, int lane, int64_t index,
                    const std::string& prev_hash, const std::string& items,
                    int64_t frontier, const std::string& m_digest) {
  uint8_t content[PEARL_CAR_CONTENT_LEN];
  std::string items_digest = TeeHost::Sha256(items);
  pearl_car_content(content, domain, static_cast<uint32_t>(lane), index,
                    U8(Id32(prev_hash)), U8(items_digest), frontier,
                    U8(Id32(m_digest)));
  return TeeHost::Sha256(
      std::string(reinterpret_cast<char*>(content), sizeof(content)));
}

}  // namespace

/* ---- Lifecycle ---- */

TeeHost::~TeeHost() {
  if (ok_) sgx_destroy_enclave(static_cast<sgx_enclave_id_t>(eid_));
}

bool TeeHost::Initialize(const std::string& enclave_path, int self_id, int n,
                         int f, bool bof, const std::string& group_seed) {
  sgx_launch_token_t token = {};
  int updated = 0;
  sgx_enclave_id_t eid;
  sgx_status_t ret = sgx_create_enclave(enclave_path.c_str(),
                                        SGX_DEBUG_FLAG, /* debug/sim mode */
                                        &token, &updated, &eid, nullptr);
  if (ret != SGX_SUCCESS) {
    fprintf(stderr, "[TEE] sgx_create_enclave(%s) failed: 0x%x\n",
            enclave_path.c_str(), ret);
    return false;
  }
  eid_ = static_cast<uint64_t>(eid);

  sgx_status_t ecall_ret;
  std::string seed = Id32(group_seed);
  ret = ecall_init(eid, &ecall_ret, static_cast<uint32_t>(self_id),
                   static_cast<uint32_t>(n), static_cast<uint32_t>(f),
                   bof ? 1u : 0u, U8(seed));
  if (ret != SGX_SUCCESS || ecall_ret != SGX_SUCCESS) {
    fprintf(stderr, "[TEE] ecall_init failed: 0x%x / 0x%x\n", ret, ecall_ret);
    sgx_destroy_enclave(eid);
    return false;
  }
  n_ = n;
  bof_ = bof;
  ok_ = true;
  return true;
}

/* ---- Pearl-OL ---- */

bool TeeHost::OlAttest(const std::string& id, int64_t* ts, std::string* tag) {
  if (!ok_) return false;
  std::string id32 = Id32(id);
  uint8_t out_tag[PEARL_TAG_LEN];
  sgx_status_t ecall_ret;
  std::unique_lock<std::mutex> lk(mu_);
  /* The host's clock reading, bumped past the last timestamp it fed the
   * enclave, which accepts only strictly increasing values. */
  *ts = std::max(WallClockUs(), last_ts_ + 1);
  sgx_status_t ret = ecall_ol_attest(static_cast<sgx_enclave_id_t>(eid_),
                                     &ecall_ret, U8(id32), *ts, out_tag);
  if (ret == SGX_SUCCESS && ecall_ret == SGX_SUCCESS) last_ts_ = *ts;
  lk.unlock();
  if (!Check("ecall_ol_attest", ret, ecall_ret)) return false;
  tag->assign(reinterpret_cast<char*>(out_tag), PEARL_TAG_LEN);
  return true;
}

bool TeeHost::OlLocalKey(const std::string& id,
                         const std::vector<SignedIndicator>& timestamps,
                         int64_t* key) {
  if (!ok_) return false;
  std::string id32 = Id32(id);
  std::string recs = PackIndicators(timestamps);
  sgx_status_t ecall_ret;
  std::unique_lock<std::mutex> lk(mu_);
  sgx_status_t ret = ecall_ol_local_key(
      static_cast<sgx_enclave_id_t>(eid_), &ecall_ret, U8(id32), U8(recs),
      static_cast<uint32_t>(recs.size()), key);
  lk.unlock();
  return Check("ecall_ol_local_key", ret, ecall_ret);
}

bool TeeHost::OlSealCar(const std::vector<OlItem>& items, SealedCar* out) {
  if (!ok_) return false;
  std::string bytes = OlItemBytes(items);
  uint8_t hash[PEARL_ID_LEN], tag[PEARL_TAG_LEN];
  sgx_status_t ecall_ret;
  std::unique_lock<std::mutex> lk(mu_);
  sgx_status_t ret = ecall_ol_seal_car(
      static_cast<sgx_enclave_id_t>(eid_), &ecall_ret, U8(bytes),
      static_cast<uint32_t>(bytes.size()), &out->index, &out->frontier, hash,
      tag);
  lk.unlock();
  if (!Check("ecall_ol_seal_car", ret, ecall_ret)) return false;
  out->hash.assign(reinterpret_cast<char*>(hash), PEARL_ID_LEN);
  out->tag.assign(reinterpret_cast<char*>(tag), PEARL_TAG_LEN);
  return true;
}

/* ---- Pearl-BOF ---- */

bool TeeHost::BofPlace(const std::string& id, int64_t* car, std::string* tag) {
  if (!ok_) return false;
  std::string id32 = Id32(id);
  uint8_t out_tag[PEARL_TAG_LEN];
  sgx_status_t ecall_ret;
  std::unique_lock<std::mutex> lk(mu_);
  sgx_status_t ret = ecall_bof_place(static_cast<sgx_enclave_id_t>(eid_),
                                     &ecall_ret, U8(id32), car, out_tag);
  lk.unlock();
  if (!Check("ecall_bof_place", ret, ecall_ret)) return false;
  tag->assign(reinterpret_cast<char*>(out_tag), PEARL_TAG_LEN);
  return true;
}

bool TeeHost::BofRecordAnnouncements(
    const std::string& id, const std::vector<SignedIndicator>& placements) {
  if (!ok_) return false;
  std::string id32 = Id32(id);
  std::string recs = PackIndicators(placements);
  sgx_status_t ecall_ret;
  std::unique_lock<std::mutex> lk(mu_);
  sgx_status_t ret = ecall_bof_record_announcements(
      static_cast<sgx_enclave_id_t>(eid_), &ecall_ret, U8(id32), U8(recs),
      static_cast<uint32_t>(recs.size()));
  lk.unlock();
  return Check("ecall_bof_record_announcements", ret, ecall_ret);
}

bool TeeHost::BofSealCar(SealedCar* out) {
  if (!ok_) return false;
  // Capacity for the items of one Car; the host never lets a Car exceed its
  // block size, so this bound is generous.
  const uint32_t items_cap = 65536u * PEARL_BOF_ITEM_REC;
  std::unique_ptr<uint8_t[]> items(new uint8_t[items_cap]);
  std::string m(static_cast<size_t>(n_) * PEARL_M_REC, '\0');
  uint32_t num_items = 0;
  uint8_t hash[PEARL_ID_LEN], tag[PEARL_TAG_LEN];
  sgx_status_t ecall_ret;
  std::unique_lock<std::mutex> lk(mu_);
  sgx_status_t ret = ecall_bof_seal_car(
      static_cast<sgx_enclave_id_t>(eid_), &ecall_ret,
      items.get(), items_cap, &num_items,
      &out->index, &out->frontier, reinterpret_cast<uint8_t*>(&m[0]),
      static_cast<uint32_t>(m.size()), hash, tag);
  lk.unlock();
  if (!Check("ecall_bof_seal_car", ret, ecall_ret)) return false;

  out->bof_items.clear();
  out->bof_items.reserve(num_items);
  for (uint32_t i = 0; i < num_items; i++) {
    const char* rec =
        reinterpret_cast<const char*>(items.get()) + i * PEARL_BOF_ITEM_REC;
    BofItem it;
    it.id.assign(rec, PEARL_ID_LEN);
    it.heartbeat = rec[PEARL_ID_LEN] != 0;
    out->bof_items.push_back(std::move(it));
  }
  out->m.assign(n_ + 1, Announcement());
  for (int j = 1; j <= n_; j++) {
    const uint8_t* rec = U8(m) + (j - 1) * PEARL_M_REC;
    if (rec[0] == 0) continue;
    Announcement& a = out->m[j];
    a.present = true;
    a.car = pearl_get_i64(rec + 1);
    a.id.assign(reinterpret_cast<const char*>(rec + 9), PEARL_ID_LEN);
    a.tag.assign(reinterpret_cast<const char*>(rec + 9 + PEARL_ID_LEN),
                 PEARL_TAG_LEN);
  }
  out->hash.assign(reinterpret_cast<char*>(hash), PEARL_ID_LEN);
  out->tag.assign(reinterpret_cast<char*>(tag), PEARL_TAG_LEN);
  return true;
}

bool TeeHost::Heartbeat(std::string* id, int64_t* value) {
  if (!ok_) return false;
  uint8_t out_id[PEARL_ID_LEN];
  sgx_status_t ecall_ret;
  std::unique_lock<std::mutex> lk(mu_);
  int64_t ts = std::max(WallClockUs(), last_ts_ + 1);
  sgx_status_t ret = ecall_heartbeat(static_cast<sgx_enclave_id_t>(eid_),
                                     &ecall_ret, ts, out_id, value);
  if (ret == SGX_SUCCESS && ecall_ret == SGX_SUCCESS && !bof_) last_ts_ = ts;
  lk.unlock();
  if (!Check("ecall_heartbeat", ret, ecall_ret)) return false;
  id->assign(reinterpret_cast<char*>(out_id), PEARL_ID_LEN);
  return true;
}

bool TeeHost::Forget(const std::vector<std::string>& ids) {
  if (!ok_) return false;
  if (ids.empty()) return true;
  std::string buf;
  buf.reserve(ids.size() * PEARL_ID_LEN);
  for (const std::string& id : ids) buf += Id32(id);
  sgx_status_t ecall_ret;
  std::unique_lock<std::mutex> lk(mu_);
  sgx_status_t ret = ecall_forget(static_cast<sgx_enclave_id_t>(eid_), &ecall_ret,
                                  U8(buf), static_cast<uint32_t>(buf.size()));
  lk.unlock();
  return Check("ecall_forget", ret, ecall_ret);
}

bool TeeHost::SignBlock(int64_t view, int64_t height, const std::string& digest,
                        std::string* tag) {
  if (!ok_) return false;
  std::string d32 = Id32(digest);
  uint8_t out_tag[PEARL_TAG_LEN];
  sgx_status_t ecall_ret;
  std::unique_lock<std::mutex> lk(mu_);
  sgx_status_t ret = ecall_sign_block(static_cast<sgx_enclave_id_t>(eid_),
                                      &ecall_ret, view, height, U8(d32), out_tag);
  lk.unlock();
  if (!Check("ecall_sign_block", ret, ecall_ret)) return false;
  tag->assign(reinterpret_cast<char*>(out_tag), PEARL_TAG_LEN);
  return true;
}

std::vector<bool> TeeHost::VerifyBatch(const std::vector<VerifyItem>& items) {
  std::vector<bool> result(items.size(), false);
  if (!ok_ || items.empty()) return result;
  std::string recs;
  for (const auto& it : items) {
    uint8_t len[4];
    pearl_put_u32(len, static_cast<uint32_t>(it.msg.size()));
    recs.append(reinterpret_cast<char*>(len), 4);
    recs.append(it.msg);
    std::string tag(PEARL_TAG_LEN, '\0');
    std::memcpy(&tag[0], it.tag.data(), std::min<size_t>(it.tag.size(), PEARL_TAG_LEN));
    recs.append(tag);
  }
  std::vector<uint8_t> ok(items.size(), 0);
  sgx_status_t ecall_ret;
  std::unique_lock<std::mutex> lk(mu_);
  sgx_status_t ret = ecall_verify_batch(
      static_cast<sgx_enclave_id_t>(eid_), &ecall_ret, U8(recs),
      static_cast<uint32_t>(recs.size()), ok.data(),
      static_cast<uint32_t>(ok.size()));
  lk.unlock();
  if (!Check("ecall_verify_batch", ret, ecall_ret)) return result;
  for (size_t i = 0; i < items.size(); i++) result[i] = ok[i] != 0;
  return result;
}

/* ---- Message builders ---- */

std::string TeeHost::OlTimestampMsg(int signer, const std::string& id,
                                    int64_t ts) {
  uint8_t msg[PEARL_INDICATOR_MSG_LEN];
  pearl_indicator_msg(msg, PEARL_DOM_OL_TIMESTAMP, static_cast<uint32_t>(signer),
                      U8(Id32(id)), ts);
  return std::string(reinterpret_cast<char*>(msg), sizeof(msg));
}

std::string TeeHost::BofPlacementMsg(int signer, const std::string& id,
                                     int64_t car) {
  uint8_t msg[PEARL_INDICATOR_MSG_LEN];
  pearl_indicator_msg(msg, PEARL_DOM_BOF_PLACEMENT, static_cast<uint32_t>(signer),
                      U8(Id32(id)), car);
  return std::string(reinterpret_cast<char*>(msg), sizeof(msg));
}

std::string TeeHost::CarSigMsg(int lane, const std::string& car_hash) {
  uint8_t msg[PEARL_CAR_SIG_MSG_LEN];
  pearl_car_sig_msg(msg, static_cast<uint32_t>(lane), U8(Id32(car_hash)));
  return std::string(reinterpret_cast<char*>(msg), sizeof(msg));
}

std::string TeeHost::BlockMsg(int signer, int64_t view, int64_t height,
                              const std::string& digest) {
  uint8_t msg[PEARL_BLOCK_MSG_LEN];
  pearl_block_msg(msg, static_cast<uint32_t>(signer), view, height,
                  U8(Id32(digest)));
  return std::string(reinterpret_cast<char*>(msg), sizeof(msg));
}

std::string TeeHost::OlCarHash(int lane, int64_t index,
                               const std::string& prev_hash,
                               const std::vector<OlItem>& items,
                               int64_t frontier) {
  return CarHash(PEARL_DOM_CAR_CONTENT_OL, lane, index, prev_hash,
                 OlItemBytes(items), frontier, std::string());
}

std::string TeeHost::BofCarHash(int lane, int64_t index,
                                const std::string& prev_hash,
                                const std::vector<BofItem>& items, int64_t w,
                                const std::vector<Announcement>& m, int n) {
  return CarHash(PEARL_DOM_CAR_CONTENT_BOF, lane, index, prev_hash,
                 BofItemBytes(items), w, Sha256(MBytes(m, n)));
}

std::string TeeHost::Sha256(const std::string& data) {
  unsigned char d[SHA256_DIGEST_LENGTH];
  SHA256(reinterpret_cast<const unsigned char*>(data.data()), data.size(), d);
  return std::string(reinterpret_cast<char*>(d), SHA256_DIGEST_LENGTH);
}

}  // namespace autobahn
}  // namespace resdb
