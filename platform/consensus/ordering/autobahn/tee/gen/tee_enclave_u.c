#include "tee_enclave_u.h"
#include <errno.h>

typedef struct ms_ecall_init_tee_t {
	sgx_status_t ms_retval;
	uint8_t* ms_pubkey_out;
} ms_ecall_init_tee_t;

typedef struct ms_ecall_timestamp_txn_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_hash;
	int64_t* ms_timestamp_out;
	uint8_t* ms_sig_out;
} ms_ecall_timestamp_txn_t;

typedef struct ms_ecall_assign_sequence_number_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_hash;
	int64_t* ms_seq_out;
	uint8_t* ms_sig_out;
} ms_ecall_assign_sequence_number_t;

typedef struct ms_ecall_sign_bytes_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_data;
	uint32_t ms_data_len;
	uint8_t* ms_sig_out;
} ms_ecall_sign_bytes_t;

typedef struct ms_ocall_get_time_t {
	int64_t* ms_t;
} ms_ocall_get_time_t;

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

static sgx_status_t SGX_CDECL tee_enclave_ocall_get_time(void* pms)
{
	ms_ocall_get_time_t* ms = SGX_CAST(ms_ocall_get_time_t*, pms);
	ocall_get_time(ms->ms_t);

	return SGX_SUCCESS;
}

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
	void * table[6];
} ocall_table_tee_enclave = {
	6,
	{
		(void*)tee_enclave_ocall_get_time,
		(void*)tee_enclave_sgx_oc_cpuidex,
		(void*)tee_enclave_sgx_thread_wait_untrusted_event_ocall,
		(void*)tee_enclave_sgx_thread_set_untrusted_event_ocall,
		(void*)tee_enclave_sgx_thread_setwait_untrusted_events_ocall,
		(void*)tee_enclave_sgx_thread_set_multiple_untrusted_events_ocall,
	}
};
sgx_status_t ecall_init_tee(sgx_enclave_id_t eid, sgx_status_t* retval, uint8_t* pubkey_out)
{
	sgx_status_t status;
	ms_ecall_init_tee_t ms;
	ms.ms_pubkey_out = pubkey_out;
	status = sgx_ecall(eid, 0, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_timestamp_txn(sgx_enclave_id_t eid, sgx_status_t* retval, const uint8_t* hash, int64_t* timestamp_out, uint8_t* sig_out)
{
	sgx_status_t status;
	ms_ecall_timestamp_txn_t ms;
	ms.ms_hash = hash;
	ms.ms_timestamp_out = timestamp_out;
	ms.ms_sig_out = sig_out;
	status = sgx_ecall(eid, 1, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_assign_sequence_number(sgx_enclave_id_t eid, sgx_status_t* retval, const uint8_t* hash, int64_t* seq_out, uint8_t* sig_out)
{
	sgx_status_t status;
	ms_ecall_assign_sequence_number_t ms;
	ms.ms_hash = hash;
	ms.ms_seq_out = seq_out;
	ms.ms_sig_out = sig_out;
	status = sgx_ecall(eid, 2, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

sgx_status_t ecall_sign_bytes(sgx_enclave_id_t eid, sgx_status_t* retval, const uint8_t* data, uint32_t data_len, uint8_t* sig_out)
{
	sgx_status_t status;
	ms_ecall_sign_bytes_t ms;
	ms.ms_data = data;
	ms.ms_data_len = data_len;
	ms.ms_sig_out = sig_out;
	status = sgx_ecall(eid, 3, &ocall_table_tee_enclave, &ms);
	if (status == SGX_SUCCESS && retval) *retval = ms.ms_retval;
	return status;
}

