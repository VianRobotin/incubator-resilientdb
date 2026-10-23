/* aes_cmac.h — AES-128-CMAC (RFC 4493) with AES-NI.
 *
 * The enclave authenticates its messages with a MAC under the committee's
 * group key.  CMAC over AES-NI costs a few AES rounds per 16-byte block,
 * against two SHA-256 compressions plus context setup for HMAC-SHA256.
 *
 * Uses GCC vector builtins rather than <wmmintrin.h>, because enclave code
 * is compiled with -nostdinc.  Compile with -maes.
 */
#ifndef PEARL_AES_CMAC_H_
#define PEARL_AES_CMAC_H_

#include <stdint.h>
#include <string.h>

namespace pearl {

typedef long long cmac_v2di __attribute__((vector_size(16)));
typedef int cmac_v4si __attribute__((vector_size(16)));

class AesCmac {
 public:
  static const int kTagLen = 16;

  void SetKey(const uint8_t key[16]) {
    rk_[0] = Load(key);
    rk_[1] = Expand(rk_[0], __builtin_ia32_aeskeygenassist128(rk_[0], 0x01));
    rk_[2] = Expand(rk_[1], __builtin_ia32_aeskeygenassist128(rk_[1], 0x02));
    rk_[3] = Expand(rk_[2], __builtin_ia32_aeskeygenassist128(rk_[2], 0x04));
    rk_[4] = Expand(rk_[3], __builtin_ia32_aeskeygenassist128(rk_[3], 0x08));
    rk_[5] = Expand(rk_[4], __builtin_ia32_aeskeygenassist128(rk_[4], 0x10));
    rk_[6] = Expand(rk_[5], __builtin_ia32_aeskeygenassist128(rk_[5], 0x20));
    rk_[7] = Expand(rk_[6], __builtin_ia32_aeskeygenassist128(rk_[6], 0x40));
    rk_[8] = Expand(rk_[7], __builtin_ia32_aeskeygenassist128(rk_[7], 0x80));
    rk_[9] = Expand(rk_[8], __builtin_ia32_aeskeygenassist128(rk_[8], 0x1b));
    rk_[10] = Expand(rk_[9], __builtin_ia32_aeskeygenassist128(rk_[9], 0x36));

    /* Subkeys K1, K2 from L = AES_K(0^128). */
    uint8_t l[16];
    cmac_v2di zero = {0, 0};
    Store(l, Encrypt(zero));
    Double(l, k1_);
    Double(k1_, k2_);
  }

  void Mac(const uint8_t* msg, uint32_t len, uint8_t tag[16]) const {
    uint32_t blocks = len == 0 ? 1 : (len + 15) / 16;
    bool complete = len > 0 && len % 16 == 0;
    cmac_v2di x = {0, 0};
    for (uint32_t i = 0; i + 1 < blocks; i++) {
      x = Encrypt(x ^ Load(msg + 16 * i));
    }
    uint8_t last[16];
    uint32_t off = 16 * (blocks - 1);
    if (complete) {
      for (int i = 0; i < 16; i++) last[i] = msg[off + i] ^ k1_[i];
    } else {
      uint32_t rem = len - off;
      memset(last, 0, 16);
      memcpy(last, msg + off, rem);
      last[rem] = 0x80;
      for (int i = 0; i < 16; i++) last[i] ^= k2_[i];
    }
    Store(tag, Encrypt(x ^ Load(last)));
  }

  /* Constant-time comparison against a received tag. */
  bool Valid(const uint8_t* msg, uint32_t len, const uint8_t* tag) const {
    uint8_t expect[16];
    Mac(msg, len, expect);
    uint8_t diff = 0;
    for (int i = 0; i < 16; i++) diff |= expect[i] ^ tag[i];
    return diff == 0;
  }

 private:
  static cmac_v2di Load(const uint8_t* p) {
    cmac_v2di v;
    memcpy(&v, p, 16);
    return v;
  }
  static void Store(uint8_t* p, cmac_v2di v) { memcpy(p, &v, 16); }

  static cmac_v2di Expand(cmac_v2di key, cmac_v2di assist) {
    assist = (cmac_v2di)__builtin_ia32_pshufd((cmac_v4si)assist, 0xff);
    key ^= __builtin_ia32_pslldqi128(key, 32);
    key ^= __builtin_ia32_pslldqi128(key, 32);
    key ^= __builtin_ia32_pslldqi128(key, 32);
    return key ^ assist;
  }

  cmac_v2di Encrypt(cmac_v2di block) const {
    block ^= rk_[0];
    for (int i = 1; i < 10; i++) block = __builtin_ia32_aesenc128(block, rk_[i]);
    return __builtin_ia32_aesenclast128(block, rk_[10]);
  }

  /* Multiplication by x in GF(2^128), big-endian bit order (RFC 4493). */
  static void Double(const uint8_t in[16], uint8_t out[16]) {
    uint8_t carry = in[0] >> 7;
    for (int i = 0; i < 15; i++) out[i] = (uint8_t)((in[i] << 1) | (in[i + 1] >> 7));
    out[15] = (uint8_t)(in[15] << 1);
    if (carry) out[15] ^= 0x87;
  }

  cmac_v2di rk_[11];
  uint8_t k1_[16];
  uint8_t k2_[16];
};

}  // namespace pearl

#endif  // PEARL_AES_CMAC_H_
