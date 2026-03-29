/* tee_host.cpp — Untrusted host wrapper for the AutoBahn TEE enclave.
 *
 * Compiled by the standalone Makefile into libtee_host.a.
 * No glog dependency: uses fprintf(stderr) for diagnostics.
 *
 * Byte-order note:
 *   SGX stores EC keys and signature components in little-endian byte order.
 *   OpenSSL BN_bin2bn() expects big-endian.  reverse32() handles the conversion.
 */

#include "platform/consensus/ordering/autobahn/tee/tee_host.h"
#include "platform/consensus/ordering/autobahn/tee/gen/tee_enclave_u.h"

#include <openssl/bn.h>
#include <openssl/ec.h>
#include <openssl/ecdsa.h>
#include <openssl/obj_mac.h>
#include <openssl/sha.h>
#include <sgx_urts.h>
#include <sys/time.h>

#include <algorithm>
#include <cstdio>
#include <cstring>

/* ---- OCALL implementation — must have C linkage (called by generated stub) ---- */
extern "C" void ocall_get_time(int64_t* t) {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    *t = static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

namespace resdb {
namespace autobahn {

/* ---- Lifecycle ---- */

TeeHost::~TeeHost() {
    if (ok_) sgx_destroy_enclave(static_cast<sgx_enclave_id_t>(eid_));
}

bool TeeHost::Initialize(const std::string& enclave_path) {
    sgx_launch_token_t token  = {};
    int                updated = 0;

    sgx_enclave_id_t eid;
    sgx_status_t ret = sgx_create_enclave(
            enclave_path.c_str(),
            SGX_DEBUG_FLAG,   /* 1 = debug/sim mode; set to 0 for production */
            &token, &updated,
            &eid, nullptr);

    if (ret != SGX_SUCCESS) {
        fprintf(stderr, "[TEE] sgx_create_enclave(%s) failed: 0x%x\n",
                enclave_path.c_str(), ret);
        return false;
    }
    eid_ = static_cast<uint64_t>(eid);

    uint8_t    pubkey[64] = {};
    sgx_status_t ecall_ret;
    ret = ecall_init_tee(eid, &ecall_ret, pubkey);

    if (ret != SGX_SUCCESS || ecall_ret != SGX_SUCCESS) {
        fprintf(stderr, "[TEE] ecall_init_tee failed: 0x%x / 0x%x\n",
                ret, ecall_ret);
        sgx_destroy_enclave(eid);
        return false;
    }

    pubkey_ = std::string(reinterpret_cast<char*>(pubkey), 64);
    ok_     = true;
    fprintf(stderr, "[TEE] SGX enclave initialized (simulation mode), eid=%llu\n",
            static_cast<unsigned long long>(eid_));
    return true;
}

/* ---- Timestamp ECALL ---- */

bool TeeHost::Timestamp(const std::string& txn_hash_bytes, Result* out) {
    if (!ok_) return false;

    uint8_t hash_buf[32] = {};
    size_t  copy = std::min(txn_hash_bytes.size(), static_cast<size_t>(32));
    std::memcpy(hash_buf, txn_hash_bytes.data(), copy);

    int64_t    timestamp = 0;
    uint8_t    sig[64]   = {};
    sgx_status_t ecall_ret;

    std::unique_lock<std::mutex> lk(mu_);
    sgx_status_t ret = ecall_timestamp_txn(
            static_cast<sgx_enclave_id_t>(eid_),
            &ecall_ret,
            hash_buf, &timestamp, sig);
    lk.unlock();

    if (ret != SGX_SUCCESS) {
        fprintf(stderr, "[TEE] ecall_timestamp_txn SGX error: 0x%x\n", ret);
        return false;
    }
    if (ecall_ret == SGX_ERROR_INVALID_PARAMETER) {
        return false;  /* duplicate — already attested */
    }
    if (ecall_ret != SGX_SUCCESS) {
        fprintf(stderr, "[TEE] ecall_timestamp_txn enclave error: 0x%x\n", ecall_ret);
        return false;
    }

    out->timestamp = timestamp;
    out->sig       = std::string(reinterpret_cast<char*>(sig), 64);
    return true;
}

/* ---- SequenceNumber ECALL (BOF / Algorithm 4) ---- */

bool TeeHost::SequenceNumber(const std::string& txn_hash_bytes, Result* out) {
    if (!ok_) return false;

    uint8_t hash_buf[32] = {};
    size_t  copy = std::min(txn_hash_bytes.size(), static_cast<size_t>(32));
    std::memcpy(hash_buf, txn_hash_bytes.data(), copy);

    int64_t    seq    = 0;
    uint8_t    sig[64] = {};
    sgx_status_t ecall_ret;

    std::unique_lock<std::mutex> lk(mu_);
    sgx_status_t ret = ecall_assign_sequence_number(
            static_cast<sgx_enclave_id_t>(eid_),
            &ecall_ret,
            hash_buf, &seq, sig);
    lk.unlock();

    if (ret != SGX_SUCCESS) {
        fprintf(stderr, "[TEE] ecall_assign_sequence_number SGX error: 0x%x\n", ret);
        return false;
    }
    if (ecall_ret == SGX_ERROR_INVALID_PARAMETER) {
        return false;  /* duplicate */
    }
    if (ecall_ret != SGX_SUCCESS) {
        fprintf(stderr, "[TEE] ecall_assign_sequence_number enclave error: 0x%x\n", ecall_ret);
        return false;
    }

    out->timestamp = seq;
    out->sig       = std::string(reinterpret_cast<char*>(sig), 64);
    return true;
}

/* ---- SignBytes ECALL (TEE-attested L⃗) ---- */

bool TeeHost::SignBytes(const std::string& data, std::string* sig64_out) {
    if (!ok_) return false;
    if (data.empty() || data.size() > 1024) return false;

    uint8_t    sig[64] = {};
    sgx_status_t ecall_ret;

    std::unique_lock<std::mutex> lk(mu_);
    sgx_status_t ret = ecall_sign_bytes(
            static_cast<sgx_enclave_id_t>(eid_),
            &ecall_ret,
            reinterpret_cast<const uint8_t*>(data.data()),
            static_cast<uint32_t>(data.size()),
            sig);
    lk.unlock();

    if (ret != SGX_SUCCESS || ecall_ret != SGX_SUCCESS) {
        fprintf(stderr, "[TEE] ecall_sign_bytes failed: 0x%x / 0x%x\n", ret, ecall_ret);
        return false;
    }

    *sig64_out = std::string(reinterpret_cast<char*>(sig), 64);
    return true;
}

/* ---- Signature verification (OpenSSL, untrusted side) ---- */

/* Reverse 32 bytes: SGX little-endian → OpenSSL big-endian. */
static void reverse32(const uint8_t* src, uint8_t* dst) {
    for (int i = 0; i < 32; i++) dst[i] = src[31 - i];
}

/* Build an OpenSSL EC_KEY from 32-byte (x, y) pair, trying both byte orderings.
 * SGX documentation says little-endian, but simulation mode may differ.
 * Returns a valid EC_KEY or nullptr if neither ordering yields a point on the curve. */
static EC_KEY* build_ec_key(const uint8_t* x32, const uint8_t* y32) {
    uint8_t x_rev[32], y_rev[32];
    reverse32(x32, x_rev);
    reverse32(y32, y_rev);

    /* Try reversed (SGX LE → BE) first, then as-is. */
    for (int attempt = 0; attempt < 2; attempt++) {
        const uint8_t* xb = (attempt == 0) ? x_rev : x32;
        const uint8_t* yb = (attempt == 0) ? y_rev : y32;

        EC_KEY* key = EC_KEY_new_by_curve_name(NID_X9_62_prime256v1);
        if (!key) continue;
        BIGNUM* bx = BN_bin2bn(xb, 32, nullptr);
        BIGNUM* by = BN_bin2bn(yb, 32, nullptr);
        if (bx && by && EC_KEY_set_public_key_affine_coordinates(key, bx, by)) {
            BN_free(bx); BN_free(by);
            return key;   /* point is on the curve — use this ordering */
        }
        BN_free(bx); BN_free(by); EC_KEY_free(key);
    }
    return nullptr;
}

bool TeeHost::Verify(const std::string& txn_hash_bytes,
                     int64_t            timestamp,
                     const std::string& sig64,
                     const std::string& pubkey64) {
    if (sig64.size() != 64 || pubkey64.size() != 64) return false;

    /* Reconstruct the signed message (must match enclave's ecall_timestamp_txn). */
    uint8_t msg[40] = {};
    size_t  copy    = std::min(txn_hash_bytes.size(), static_cast<size_t>(32));
    std::memcpy(msg, txn_hash_bytes.data(), copy);
    for (int i = 0; i < 8; i++) msg[32 + i] = static_cast<uint8_t>(timestamp >> (i * 8));

    /* sgx_ecdsa_sign() hashes internally (SHA-256), so we passed the
     * pre-computed digest to it, meaning it effectively signed
     * SHA-256(SHA-256(msg)).  Replicate that double-hash here. */
    uint8_t d1[32], digest[32];
    SHA256(msg, sizeof(msg), d1);
    SHA256(d1, sizeof(d1), digest);

    /* Reconstruct the public key, trying both byte orderings. */
    const auto* gx = reinterpret_cast<const uint8_t*>(pubkey64.data());
    const auto* gy = gx + 32;
    EC_KEY* key = build_ec_key(gx, gy);
    if (!key) return false;

    /* Try both byte orderings for r and s as well. */
    const auto* r_raw = reinterpret_cast<const uint8_t*>(sig64.data());
    const auto* s_raw = r_raw + 32;
    uint8_t r_rev[32], s_rev[32];
    reverse32(r_raw, r_rev);
    reverse32(s_raw, s_rev);

    int result = 0;
    for (int attempt = 0; attempt < 2 && result != 1; attempt++) {
        const uint8_t* rb = (attempt == 0) ? r_rev : r_raw;
        const uint8_t* sb = (attempt == 0) ? s_rev : s_raw;
        BIGNUM* br = BN_bin2bn(rb, 32, nullptr);
        BIGNUM* bs = BN_bin2bn(sb, 32, nullptr);
        if (!br || !bs) { BN_free(br); BN_free(bs); continue; }
        ECDSA_SIG* ec_sig = ECDSA_SIG_new();
        ECDSA_SIG_set0(ec_sig, br, bs);
        result = ECDSA_do_verify(digest, 32, ec_sig, key);
        ECDSA_SIG_free(ec_sig);
    }

    EC_KEY_free(key);
    return result == 1;
}

/* Verify a signature produced by ecall_sign_bytes.
 * sgx_ecdsa_sign hashes the data internally (single SHA-256), so we just
 * compute SHA-256(data) and call ECDSA_do_verify — no pre-hash needed. */
bool TeeHost::VerifyBytes(const std::string& data,
                          const std::string& sig64,
                          const std::string& pubkey64) {
    if (sig64.size() != 64 || pubkey64.size() != 64) return false;

    uint8_t digest[32];
    SHA256(reinterpret_cast<const uint8_t*>(data.data()), data.size(), digest);

    const auto* gx = reinterpret_cast<const uint8_t*>(pubkey64.data());
    const auto* gy = gx + 32;
    EC_KEY* key = build_ec_key(gx, gy);
    if (!key) return false;

    const auto* r_raw = reinterpret_cast<const uint8_t*>(sig64.data());
    const auto* s_raw = r_raw + 32;
    uint8_t r_rev[32], s_rev[32];
    reverse32(r_raw, r_rev);
    reverse32(s_raw, s_rev);

    int result = 0;
    for (int attempt = 0; attempt < 2 && result != 1; attempt++) {
        const uint8_t* rb = (attempt == 0) ? r_rev : r_raw;
        const uint8_t* sb = (attempt == 0) ? s_rev : s_raw;
        BIGNUM* br = BN_bin2bn(rb, 32, nullptr);
        BIGNUM* bs = BN_bin2bn(sb, 32, nullptr);
        if (!br || !bs) { BN_free(br); BN_free(bs); continue; }
        ECDSA_SIG* ec_sig = ECDSA_SIG_new();
        ECDSA_SIG_set0(ec_sig, br, bs);
        result = ECDSA_do_verify(digest, 32, ec_sig, key);
        ECDSA_SIG_free(ec_sig);
    }

    EC_KEY_free(key);
    return result == 1;
}

}  // namespace autobahn
}  // namespace resdb
