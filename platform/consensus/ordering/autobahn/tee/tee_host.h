#pragma once
/* tee_host.h — Untrusted host wrapper for the AutoBahn TEE enclave.
 *
 * Deliberately avoids including <sgx_urts.h> so that any translation unit
 * that only needs the type declarations does not depend on the SGX SDK headers.
 * The SGX SDK is only required by tee_host.cpp.
 */

#include <cstdint>
#include <mutex>
#include <string>

namespace resdb {
namespace autobahn {

class TeeHost {
 public:
  TeeHost() = default;
  ~TeeHost();

  /* Load the signed enclave .so and initialize the ECDSA key pair.
   * enclave_path: filesystem path to tee_enclave.signed.so
   * Returns true on success.
   */
  bool Initialize(const std::string& enclave_path);

  /* 64-byte ECDSA-P256 public key: gx[32] || gy[32] in SGX little-endian.
   * Empty until Initialize() succeeds.
   */
  std::string GetPublicKey() const { return pubkey_; }

  bool IsOk() const { return ok_; }

  /* Result of a successful timestamping ECALL. */
  struct Result {
    int64_t     timestamp;  /* microseconds since epoch */
    std::string sig;        /* 64 bytes: r[32]||s[32] in SGX little-endian */
  };

  /* Timestamp a transaction.
   * txn_hash_bytes: raw bytes of the transaction hash (up to 32 bytes used).
   * out: populated on success.
   * Returns false if the transaction was already attested (duplicate) or on error.
   */
  bool Timestamp(const std::string& txn_hash_bytes, Result* out);

  /* Assign a monotonically increasing sequence number (BOF / Algorithm 4).
   * seq field of Result carries the sequence number instead of a timestamp.
   * Returns false on duplicate or error.
   */
  bool SequenceNumber(const std::string& txn_hash_bytes, Result* out);

  /* Sign arbitrary bytes with the enclave's ECDSA-P256 key.
   * Used for TEE-attested last-seen vector L⃗ (Section V-B).
   * data_len must be > 0 and ≤ 1024.
   * Returns 64-byte r||s in sig64_out on success.
   */
  bool SignBytes(const std::string& data, std::string* sig64_out);

  /* Verify a TEE ECDSA-P256 signature (static — no enclave required).
   *
   * txn_hash_bytes: raw hash bytes (up to 32 bytes)
   * timestamp:      the timestamp value that was signed
   * sig64:          64-byte r||s in SGX little-endian format
   * pubkey64:       64-byte gx||gy in SGX little-endian format
   *
   * The signed message is SHA-256(hash[32] || timestamp_le[8]).
   * Returns true if the signature is valid.
   */
  static bool Verify(const std::string& txn_hash_bytes,
                     int64_t            timestamp,
                     const std::string& sig64,
                     const std::string& pubkey64);

  /* Verify a TEE ECDSA-P256 signature over arbitrary bytes (static).
   * Matches SignBytes(): sgx_ecdsa_sign hashes internally, so verification
   * computes SHA-256(data) and calls ECDSA_do_verify.
   */
  static bool VerifyBytes(const std::string& data,
                          const std::string& sig64,
                          const std::string& pubkey64);

  /* Verify an HMAC-SHA256 L⃗ signature using the enclave's secret key (ECALL).
   * sig64: the 64-byte output of SignBytes(); only the first 32 bytes (the MAC)
   * are used.  Requires IsOk() == true.
   */
  bool VerifyBytes(const std::string& data, const std::string& sig64) const;

 private:
  uint64_t    eid_  = 0;    /* sgx_enclave_id_t is typedef uint64_t */
  bool        ok_   = false;
  std::string pubkey_;
  std::mutex  mu_;
};

}  // namespace autobahn
}  // namespace resdb
