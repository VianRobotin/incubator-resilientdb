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

sgx_status_t ecall_init(uint32_t self_id, uint32_t n, uint32_t f, uint32_t mode, const uint8_t* group_seed);
sgx_status_t ecall_ol_attest_batch(const uint8_t* ids, uint32_t ids_len, const uint8_t* ts, uint32_t ts_len, uint8_t* tags_out, uint32_t tags_len, uint8_t* ok_out, uint32_t count);
sgx_status_t ecall_ol_local_key_batch(const uint8_t* buf, uint32_t len, uint8_t* keys_out, uint32_t keys_len, uint8_t* ok_out, uint32_t count);
sgx_status_t ecall_ol_seal_car(const uint8_t* items, uint32_t items_len, int64_t* index_out, int64_t* frontier_out, uint8_t* car_hash_out, uint8_t* tag_out);
sgx_status_t ecall_bof_place_batch(const uint8_t* ids, uint32_t ids_len, uint8_t* cars_out, uint32_t cars_len, uint8_t* tags_out, uint32_t tags_len, uint8_t* ok_out, uint32_t count);
sgx_status_t ecall_bof_record_announcements_batch(const uint8_t* buf, uint32_t len, uint8_t* ok_out, uint32_t count);
sgx_status_t ecall_bof_seal_car(uint8_t* items_out, uint32_t items_cap, uint32_t* num_items_out, int64_t* index_out, int64_t* w_out, uint8_t* m_out, uint32_t m_cap, uint8_t* car_hash_out, uint8_t* tag_out);
sgx_status_t ecall_heartbeat(int64_t ts, uint8_t* id_out, int64_t* value_out);
sgx_status_t ecall_forget(const uint8_t* ids, uint32_t ids_len);
sgx_status_t ecall_sign_block(int64_t view, int64_t height, const uint8_t* digest, uint8_t* tag_out);
sgx_status_t ecall_verify_batch(const uint8_t* records, uint32_t rec_len, uint8_t* ok_out, uint32_t count);

sgx_status_t SGX_CDECL sgx_oc_cpuidex(int cpuinfo[4], int leaf, int subleaf);
sgx_status_t SGX_CDECL sgx_thread_wait_untrusted_event_ocall(int* retval, const void* self);
sgx_status_t SGX_CDECL sgx_thread_set_untrusted_event_ocall(int* retval, const void* waiter);
sgx_status_t SGX_CDECL sgx_thread_setwait_untrusted_events_ocall(int* retval, const void* waiter, const void* self);
sgx_status_t SGX_CDECL sgx_thread_set_multiple_untrusted_events_ocall(int* retval, const void** waiters, size_t total);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
