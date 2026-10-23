/* tee_msg.h — byte layouts of every message the Pearl enclave authenticates.
 *
 * Shared by the enclave (C++11, SGX tlibc) and the untrusted host, so both
 * sides build bit-identical messages: the enclave MACs them, and a verifier
 * rebuilds the same bytes from the fields it received and asks the enclave to
 * check the tag (TEE.Verify in the paper's Alg. 1).
 *
 * All integers are little-endian.  Identifiers and digests are 32 bytes.
 * Only <stdint.h>/<string.h> are used so the header compiles inside SGX.
 */
#ifndef PEARL_TEE_MSG_H_
#define PEARL_TEE_MSG_H_

#include <stdint.h>
#include <string.h>

#define PEARL_ID_LEN  32
#define PEARL_TAG_LEN 32
#define PEARL_MAX_REPLICAS 64

/* Domain separators: the first byte of every authenticated message. */
enum PearlDomain {
  PEARL_DOM_OL_TIMESTAMP = 1,  /* σ_OL = Sign(t, T)               */
  PEARL_DOM_BOF_PLACEMENT = 2, /* σ_BOF = Sign(t, c)              */
  PEARL_DOM_CAR = 3,           /* Car seal (content + frontier)    */
  PEARL_DOM_BLOCK = 4,         /* TEE.SignBlock(tipcut, claims)    */
  PEARL_DOM_CAR_CONTENT_OL = 10,
  PEARL_DOM_CAR_CONTENT_BOF = 11,
  PEARL_DOM_HEARTBEAT_ID = 12
};

/* Record sizes of the packed arrays passed across the enclave boundary. */
#define PEARL_SIGNED_TS_REC   (4 + 8 + PEARL_TAG_LEN)          /* signer, T, tag   */
#define PEARL_SIGNED_PLACE_REC (4 + 8 + PEARL_TAG_LEN)         /* signer, c, tag   */
#define PEARL_OL_ITEM_REC     (PEARL_ID_LEN + 8)                /* id, K_r(t)       */
#define PEARL_BOF_ITEM_REC    (PEARL_ID_LEN + 1)                /* id, heartbeat    */
#define PEARL_M_REC           (1 + 8 + PEARL_ID_LEN + PEARL_TAG_LEN) /* present, c, id, tag */

static inline void pearl_put_u32(uint8_t* p, uint32_t v) {
  for (int i = 0; i < 4; i++) p[i] = (uint8_t)(v >> (8 * i));
}
static inline void pearl_put_i64(uint8_t* p, int64_t v) {
  uint64_t u = (uint64_t)v;
  for (int i = 0; i < 8; i++) p[i] = (uint8_t)(u >> (8 * i));
}
static inline uint32_t pearl_get_u32(const uint8_t* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; i++) v |= (uint32_t)p[i] << (8 * i);
  return v;
}
static inline int64_t pearl_get_i64(const uint8_t* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; i++) v |= (uint64_t)p[i] << (8 * i);
  return (int64_t)v;
}

/* (domain, signer, id, value): OL timestamps and BOF placements. */
#define PEARL_INDICATOR_MSG_LEN (1 + 4 + PEARL_ID_LEN + 8)
static inline void pearl_indicator_msg(uint8_t* out, uint8_t domain,
                                       uint32_t signer, const uint8_t* id,
                                       int64_t value) {
  out[0] = domain;
  pearl_put_u32(out + 1, signer);
  memcpy(out + 5, id, PEARL_ID_LEN);
  pearl_put_i64(out + 5 + PEARL_ID_LEN, value);
}

/* Car content that the Car hash commits to.  `frontier` is G_r in OL and w_r
 * in BOF; `m_digest` is all-zero in OL. */
#define PEARL_CAR_CONTENT_LEN (1 + 4 + 8 + PEARL_ID_LEN + PEARL_ID_LEN + 8 + PEARL_ID_LEN)
static inline void pearl_car_content(uint8_t* out, uint8_t domain,
                                     uint32_t lane, int64_t index,
                                     const uint8_t* prev_hash,
                                     const uint8_t* items_digest,
                                     int64_t frontier,
                                     const uint8_t* m_digest) {
  uint8_t* p = out;
  *p++ = domain;
  pearl_put_u32(p, lane); p += 4;
  pearl_put_i64(p, index); p += 8;
  memcpy(p, prev_hash, PEARL_ID_LEN); p += PEARL_ID_LEN;
  memcpy(p, items_digest, PEARL_ID_LEN); p += PEARL_ID_LEN;
  pearl_put_i64(p, frontier); p += 8;
  memcpy(p, m_digest, PEARL_ID_LEN);
}

/* (domain, signer, digest): the tag over a Car hash. */
#define PEARL_CAR_SIG_MSG_LEN (1 + 4 + PEARL_ID_LEN)
static inline void pearl_car_sig_msg(uint8_t* out, uint32_t lane,
                                     const uint8_t* car_hash) {
  out[0] = PEARL_DOM_CAR;
  pearl_put_u32(out + 1, lane);
  memcpy(out + 5, car_hash, PEARL_ID_LEN);
}

/* (domain, signer, view, height, digest): the tag over a consensus block. */
#define PEARL_BLOCK_MSG_LEN (1 + 4 + 8 + 8 + PEARL_ID_LEN)
static inline void pearl_block_msg(uint8_t* out, uint32_t signer, int64_t view,
                                   int64_t height, const uint8_t* digest) {
  out[0] = PEARL_DOM_BLOCK;
  pearl_put_u32(out + 1, signer);
  pearl_put_i64(out + 5, view);
  pearl_put_i64(out + 13, height);
  memcpy(out + 21, digest, PEARL_ID_LEN);
}

#endif  /* PEARL_TEE_MSG_H_ */
