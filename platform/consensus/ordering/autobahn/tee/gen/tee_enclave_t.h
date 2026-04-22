#ifndef TEE_ENCLAVE_T_H__
#define TEE_ENCLAVE_T_H__

#include <stdint.h>
#include <wchar.h>
#include <stddef.h>
#include "sgx_edger8r.h" /* for sgx_ocall etc. */


#include <stdlib.h> /* for size_t */

#define SGX_CAST(type, item) ((type)(item))

#ifdef __cplusplus
extern "C" {
#endif

sgx_status_t ecall_init_tee(uint8_t* pubkey_out);
sgx_status_t ecall_timestamp_txn(const uint8_t* hash, int64_t* timestamp_out, uint8_t* sig_out);
sgx_status_t ecall_assign_sequence_number(const uint8_t* hash, int64_t* seq_out, uint8_t* sig_out);
sgx_status_t ecall_sign_bytes(const uint8_t* data, uint32_t data_len, uint8_t* sig_out);
sgx_status_t ecall_verify_bytes(const uint8_t* data, uint32_t data_len, const uint8_t* expected_mac);

sgx_status_t SGX_CDECL ocall_get_time(int64_t* t);
sgx_status_t SGX_CDECL sgx_oc_cpuidex(int cpuinfo[4], int leaf, int subleaf);
sgx_status_t SGX_CDECL sgx_thread_wait_untrusted_event_ocall(int* retval, const void* self);
sgx_status_t SGX_CDECL sgx_thread_set_untrusted_event_ocall(int* retval, const void* waiter);
sgx_status_t SGX_CDECL sgx_thread_setwait_untrusted_events_ocall(int* retval, const void* waiter, const void* self);
sgx_status_t SGX_CDECL sgx_thread_set_multiple_untrusted_events_ocall(int* retval, const void** waiters, size_t total);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
