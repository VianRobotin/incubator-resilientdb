#include "tee_enclave_u.h"
#include <errno.h>

typedef struct ms_ecall_init_t {
	sgx_status_t ms_retval;
	uint32_t ms_self_id;
	uint32_t ms_n;
	uint32_t ms_f;
	uint32_t ms_mode;
	const uint8_t* ms_group_seed;
} ms_ecall_init_t;

typedef struct ms_ecall_ol_attest_batch_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_ids;
	uint32_t ms_ids_len;
	const uint8_t* ms_ts;
	uint32_t ms_ts_len;
	uint8_t* ms_tags_out;
	uint32_t ms_tags_len;
	uint8_t* ms_ok_out;
	uint32_t ms_count;
} ms_ecall_ol_attest_batch_t;

typedef struct ms_ecall_ol_local_key_batch_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_buf;
	uint32_t ms_len;
	uint8_t* ms_keys_out;
	uint32_t ms_keys_len;
	uint8_t* ms_ok_out;
	uint32_t ms_count;
} ms_ecall_ol_local_key_batch_t;

typedef struct ms_ecall_ol_seal_car_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_items;
	uint32_t ms_items_len;
	int64_t* ms_index_out;
	int64_t* ms_frontier_out;
	uint8_t* ms_car_hash_out;
	uint8_t* ms_tag_out;
} ms_ecall_ol_seal_car_t;

typedef struct ms_ecall_bof_place_batch_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_ids;
	uint32_t ms_ids_len;
	uint8_t* ms_cars_out;
	uint32_t ms_cars_len;
	uint8_t* ms_tags_out;
	uint32_t ms_tags_len;
	uint8_t* ms_ok_out;
	uint32_t ms_count;
} ms_ecall_bof_place_batch_t;

typedef struct ms_ecall_bof_record_announcements_batch_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_buf;
	uint32_t ms_len;
	uint8_t* ms_ok_out;
	uint32_t ms_count;
} ms_ecall_bof_record_announcements_batch_t;

typedef struct ms_ecall_bof_seal_car_t {
	sgx_status_t ms_retval;
	uint8_t* ms_items_out;
	uint32_t ms_items_cap;
	uint32_t* ms_num_items_out;
	int64_t* ms_index_out;
	int64_t* ms_w_out;
	uint8_t* ms_m_out;
	uint32_t ms_m_cap;
	uint8_t* ms_car_hash_out;
	uint8_t* ms_tag_out;
} ms_ecall_bof_seal_car_t;

typedef struct ms_ecall_heartbeat_t {
	sgx_status_t ms_retval;
	int64_t ms_ts;
	uint8_t* ms_id_out;
	int64_t* ms_value_out;
} ms_ecall_heartbeat_t;

typedef struct ms_ecall_admission_key_t {
	sgx_status_t ms_retval;
	uint8_t* ms_key_out;
} ms_ecall_admission_key_t;

typedef struct ms_ecall_sign_block_t {
	sgx_status_t ms_retval;
	int64_t ms_view;
	int64_t ms_height;
	const uint8_t* ms_digest;
	uint8_t* ms_tag_out;
} ms_ecall_sign_block_t;

typedef struct ms_ecall_verify_batch_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_records;
	uint32_t ms_rec_len;
	uint8_t* ms_ok_out;
	uint32_t ms_count;
} ms_ecall_verify_batch_t;

typedef struct ms_sgx_oc_cpuidex_t {
	int* ms_cpuinfo;
	int ms_leaf;
	int ms_subleaf;
} ms_sgx_oc_cpuidex_t;

typedef struct ms_sgx_thread_wait_untrusted_event_ocall_t {
	int ms_retval;
	const void* ms_self;
} ms_sgx_thread_wait_untrusted_event_ocall_t;

typedef struct ms_sgx_thread_set_untrusted_event_ocall_t {
	int ms_retval;
	const void* ms_waiter;
} ms_sgx_thread_set_untrusted_event_ocall_t;

typedef struct ms_sgx_thread_setwait_untrusted_events_ocall_t {
	int ms_retval;
	const void* ms_waiter;
	const void* ms_self;
} ms_sgx_thread_setwait_untrusted_events_ocall_t;

typedef struct ms_sgx_thread_set_multiple_untrusted_events_ocall_t {
	int ms_retval;
	const void** ms_waiters;
	size_t ms_total;
} ms_sgx_thread_set_multiple_untrusted_events_ocall_t;

static sgx_status_t SGX_CDECL tee_enclave_sgx_oc_cpuidex(void* pms)
{
	ms_sgx_oc_cpuidex_t* ms = SGX_CAST(ms_sgx_oc_cpuidex_t*, pms);
	sgx_oc_cpuidex(ms->ms_cpuinfo, ms->ms_leaf, ms->ms_subleaf);

	return SGX_SUCCESS;
}

static sgx_status_t SGX_CDECL tee_enclave_sgx_thread_wait_untrusted_event_ocall(void* pms)
{
	ms_sgx_thread_wait_untrusted_event_ocall_t* ms = SGX_CAST(ms_sgx_thread_wait_untrusted_event_ocall_t*, pms);
	ms->ms_retval = sgx_thread_wait_untrusted_event_ocall(ms->ms_self);

	return SGX_SUCCESS;
}

static sgx_status_t SGX_CDECL tee_enclave_sgx_thread_set_untrusted_event_ocall(void* pms)
{
	ms_sgx_thread_set_untrusted_event_ocall_t* ms = SGX_CAST(ms_sgx_thread_set_untrusted_event_ocall_t*, pms);
	ms->ms_retval = sgx_thread_set_untrusted_event_ocall(ms->ms_waiter);

	return SGX_SUCCESS;
}

static sgx_status_t SGX_CDECL tee_enclave_sgx_thread_setwait_untrusted_events_ocall(void* pms)
{
	ms_sgx_thread_setwait_untrusted_events_ocall_t* ms = SGX_CAST(ms_sgx_thread_setwait_untrusted_events_ocall_t*, pms);
	ms->ms_retval = sgx_thread_setwait_untrusted_events_ocall(ms->ms_waiter, ms->ms_self);

	return SGX_SUCCESS;
}

static sgx_status_t SGX_CDECL tee_enclave_sgx_thread_set_multiple_untrusted_events_ocall(void* pms)
{
	ms_sgx_thread_set_multiple_untrusted_events_ocall_t* ms = SGX_CAST(ms_sgx_thread_set_multiple_untrusted_events_ocall_t*, pms);
	ms->ms_retval = sgx_thread_set_multiple_untrusted_events_ocall(ms->ms_waiters, ms->ms_total);

	return SGX_SUCCESS;
}

static const struct {
	size_t nr_ocall;
	void * table[5];
} ocall_table_tee_enclave = {
	5,
	{
		(void*)tee_enclave_sgx_oc_cpuidex,
		(void*)tee_enclave_sgx_thread_wait_untrusted_event_ocall,
		(void*)tee_enclave_sgx_thread_set_untrusted_event_ocall,
		(void*)tee_enclave_sgx_thread_setwait_untrusted_events_ocall,
		(void*)tee_enclave_sgx_thread_set_multiple_untrusted_events_ocall,
	}
};
sgx_status_t ecall_init(sgx_enclave_id_t eid, sgx_status_t* retval, uint32_t self_id, uint32_t n, uint32_t f, uint32_t mode, const uint8_t* group_seed)
{
	sgx_status_t status;
	ms_ecall_init_t ms;
	ms.ms_self_id = self_id;
	ms.ms_n = n;
	ms.ms_f = f;
	ms.ms_mode = mode;
	ms.ms_group_seed = group_seed;
	status = sgx_ecall(eid, 0, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_ol_attest_batch(sgx_enclave_id_t eid, sgx_status_t* retval, const uint8_t* ids, uint32_t ids_len, const uint8_t* ts, uint32_t ts_len, uint8_t* tags_out, uint32_t tags_len, uint8_t* ok_out, uint32_t count)
{
	sgx_status_t status;
	ms_ecall_ol_attest_batch_t ms;
	ms.ms_ids = ids;
	ms.ms_ids_len = ids_len;
	ms.ms_ts = ts;
	ms.ms_ts_len = ts_len;
	ms.ms_tags_out = tags_out;
	ms.ms_tags_len = tags_len;
	ms.ms_ok_out = ok_out;
	ms.ms_count = count;
	status = sgx_ecall(eid, 1, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_ol_local_key_batch(sgx_enclave_id_t eid, sgx_status_t* retval, const uint8_t* buf, uint32_t len, uint8_t* keys_out, uint32_t keys_len, uint8_t* ok_out, uint32_t count)
{
	sgx_status_t status;
	ms_ecall_ol_local_key_batch_t ms;
	ms.ms_buf = buf;
	ms.ms_len = len;
	ms.ms_keys_out = keys_out;
	ms.ms_keys_len = keys_len;
	ms.ms_ok_out = ok_out;
	ms.ms_count = count;
	status = sgx_ecall(eid, 2, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_ol_seal_car(sgx_enclave_id_t eid, sgx_status_t* retval, const uint8_t* items, uint32_t items_len, int64_t* index_out, int64_t* frontier_out, uint8_t* car_hash_out, uint8_t* tag_out)
{
	sgx_status_t status;
	ms_ecall_ol_seal_car_t ms;
	ms.ms_items = items;
	ms.ms_items_len = items_len;
	ms.ms_index_out = index_out;
	ms.ms_frontier_out = frontier_out;
	ms.ms_car_hash_out = car_hash_out;
	ms.ms_tag_out = tag_out;
	status = sgx_ecall(eid, 3, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_bof_place_batch(sgx_enclave_id_t eid, sgx_status_t* retval, const uint8_t* ids, uint32_t ids_len, uint8_t* cars_out, uint32_t cars_len, uint8_t* tags_out, uint32_t tags_len, uint8_t* ok_out, uint32_t count)
{
	sgx_status_t status;
	ms_ecall_bof_place_batch_t ms;
	ms.ms_ids = ids;
	ms.ms_ids_len = ids_len;
	ms.ms_cars_out = cars_out;
	ms.ms_cars_len = cars_len;
	ms.ms_tags_out = tags_out;
	ms.ms_tags_len = tags_len;
	ms.ms_ok_out = ok_out;
	ms.ms_count = count;
	status = sgx_ecall(eid, 4, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_bof_record_announcements_batch(sgx_enclave_id_t eid, sgx_status_t* retval, const uint8_t* buf, uint32_t len, uint8_t* ok_out, uint32_t count)
{
	sgx_status_t status;
	ms_ecall_bof_record_announcements_batch_t ms;
	ms.ms_buf = buf;
	ms.ms_len = len;
	ms.ms_ok_out = ok_out;
	ms.ms_count = count;
	status = sgx_ecall(eid, 5, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_bof_seal_car(sgx_enclave_id_t eid, sgx_status_t* retval, uint8_t* items_out, uint32_t items_cap, uint32_t* num_items_out, int64_t* index_out, int64_t* w_out, uint8_t* m_out, uint32_t m_cap, uint8_t* car_hash_out, uint8_t* tag_out)
{
	sgx_status_t status;
	ms_ecall_bof_seal_car_t ms;
	ms.ms_items_out = items_out;
	ms.ms_items_cap = items_cap;
	ms.ms_num_items_out = num_items_out;
	ms.ms_index_out = index_out;
	ms.ms_w_out = w_out;
	ms.ms_m_out = m_out;
	ms.ms_m_cap = m_cap;
	ms.ms_car_hash_out = car_hash_out;
	ms.ms_tag_out = tag_out;
	status = sgx_ecall(eid, 6, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_heartbeat(sgx_enclave_id_t eid, sgx_status_t* retval, int64_t ts, uint8_t* id_out, int64_t* value_out)
{
	sgx_status_t status;
	ms_ecall_heartbeat_t ms;
	ms.ms_ts = ts;
	ms.ms_id_out = id_out;
	ms.ms_value_out = value_out;
	status = sgx_ecall(eid, 7, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_admission_key(sgx_enclave_id_t eid, sgx_status_t* retval, uint8_t* key_out)
{
	sgx_status_t status;
	ms_ecall_admission_key_t ms;
	ms.ms_key_out = key_out;
	status = sgx_ecall(eid, 8, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_sign_block(sgx_enclave_id_t eid, sgx_status_t* retval, int64_t view, int64_t height, const uint8_t* digest, uint8_t* tag_out)
{
	sgx_status_t status;
	ms_ecall_sign_block_t ms;
	ms.ms_view = view;
	ms.ms_height = height;
	ms.ms_digest = digest;
	ms.ms_tag_out = tag_out;
	status = sgx_ecall(eid, 9, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_verify_batch(sgx_enclave_id_t eid, sgx_status_t* retval, const uint8_t* records, uint32_t rec_len, uint8_t* ok_out, uint32_t count)
{
	sgx_status_t status;
	ms_ecall_verify_batch_t ms;
	ms.ms_records = records;
	ms.ms_rec_len = rec_len;
	ms.ms_ok_out = ok_out;
	ms.ms_count = count;
	status = sgx_ecall(eid, 10, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

