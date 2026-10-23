#include "tee_enclave_t.h"

#include "sgx_trts.h" /* for sgx_ocalloc, sgx_is_outside_enclave */
#include "sgx_lfence.h" /* for sgx_lfence */

#include <errno.h>
#include <mbusafecrt.h> /* for memcpy_s etc */
#include <stdlib.h> /* for malloc/free etc */

#define CHECK_REF_POINTER(ptr, siz) do {	\
	if (!(ptr) || ! sgx_is_outside_enclave((ptr), (siz)))	\
		return SGX_ERROR_INVALID_PARAMETER;\
} while (0)

#define CHECK_UNIQUE_POINTER(ptr, siz) do {	\
	if ((ptr) && ! sgx_is_outside_enclave((ptr), (siz)))	\
		return SGX_ERROR_INVALID_PARAMETER;\
} while (0)

#define CHECK_ENCLAVE_POINTER(ptr, siz) do {	\
	if ((ptr) && ! sgx_is_within_enclave((ptr), (siz)))	\
		return SGX_ERROR_INVALID_PARAMETER;\
} while (0)

#define ADD_ASSIGN_OVERFLOW(a, b) (	\
	((a) += (b)) < (b)	\
)


typedef struct ms_ecall_init_t {
	sgx_status_t ms_retval;
	uint32_t ms_self_id;
	uint32_t ms_n;
	uint32_t ms_f;
	uint32_t ms_mode;
	const uint8_t* ms_group_seed;
} ms_ecall_init_t;

typedef struct ms_ecall_ol_attest_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_id;
	int64_t* ms_ts_out;
	uint8_t* ms_tag_out;
} ms_ecall_ol_attest_t;

typedef struct ms_ecall_ol_local_key_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_id;
	const uint8_t* ms_records;
	uint32_t ms_rec_len;
	int64_t* ms_key_out;
} ms_ecall_ol_local_key_t;

typedef struct ms_ecall_ol_seal_car_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_items;
	uint32_t ms_items_len;
	int64_t* ms_index_out;
	int64_t* ms_frontier_out;
	uint8_t* ms_car_hash_out;
	uint8_t* ms_tag_out;
} ms_ecall_ol_seal_car_t;

typedef struct ms_ecall_bof_place_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_id;
	int64_t* ms_car_out;
	uint8_t* ms_tag_out;
} ms_ecall_bof_place_t;

typedef struct ms_ecall_bof_record_announcements_t {
	sgx_status_t ms_retval;
	const uint8_t* ms_id;
	const uint8_t* ms_records;
	uint32_t ms_rec_len;
} ms_ecall_bof_record_announcements_t;

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
	uint8_t* ms_id_out;
	int64_t* ms_value_out;
} ms_ecall_heartbeat_t;

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

static sgx_status_t SGX_CDECL sgx_ecall_init(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_init_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_init_t* ms = SGX_CAST(ms_ecall_init_t*, pms);
	ms_ecall_init_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_init_t), ms, sizeof(ms_ecall_init_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_group_seed = __in_ms.ms_group_seed;
	size_t _len_group_seed = 32;
	uint8_t* _in_group_seed = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_group_seed, _len_group_seed);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_group_seed != NULL && _len_group_seed != 0) {
		if ( _len_group_seed % sizeof(*_tmp_group_seed) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_group_seed = (uint8_t*)malloc(_len_group_seed);
		if (_in_group_seed == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_group_seed, _len_group_seed, _tmp_group_seed, _len_group_seed)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	_in_retval = ecall_init(__in_ms.ms_self_id, __in_ms.ms_n, __in_ms.ms_f, __in_ms.ms_mode, (const uint8_t*)_in_group_seed);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}

err:
	if (_in_group_seed) free(_in_group_seed);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_ol_attest(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_ol_attest_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_ol_attest_t* ms = SGX_CAST(ms_ecall_ol_attest_t*, pms);
	ms_ecall_ol_attest_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_ol_attest_t), ms, sizeof(ms_ecall_ol_attest_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_id = __in_ms.ms_id;
	size_t _len_id = 32;
	uint8_t* _in_id = NULL;
	int64_t* _tmp_ts_out = __in_ms.ms_ts_out;
	size_t _len_ts_out = sizeof(int64_t);
	int64_t* _in_ts_out = NULL;
	uint8_t* _tmp_tag_out = __in_ms.ms_tag_out;
	size_t _len_tag_out = 32;
	uint8_t* _in_tag_out = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_id, _len_id);
	CHECK_UNIQUE_POINTER(_tmp_ts_out, _len_ts_out);
	CHECK_UNIQUE_POINTER(_tmp_tag_out, _len_tag_out);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_id != NULL && _len_id != 0) {
		if ( _len_id % sizeof(*_tmp_id) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_id = (uint8_t*)malloc(_len_id);
		if (_in_id == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_id, _len_id, _tmp_id, _len_id)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_ts_out != NULL && _len_ts_out != 0) {
		if ( _len_ts_out % sizeof(*_tmp_ts_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_ts_out = (int64_t*)malloc(_len_ts_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_ts_out, 0, _len_ts_out);
	}
	if (_tmp_tag_out != NULL && _len_tag_out != 0) {
		if ( _len_tag_out % sizeof(*_tmp_tag_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_tag_out = (uint8_t*)malloc(_len_tag_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_tag_out, 0, _len_tag_out);
	}
	_in_retval = ecall_ol_attest((const uint8_t*)_in_id, _in_ts_out, _in_tag_out);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_ts_out) {
		if (memcpy_verw_s(_tmp_ts_out, _len_ts_out, _in_ts_out, _len_ts_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_tag_out) {
		if (memcpy_verw_s(_tmp_tag_out, _len_tag_out, _in_tag_out, _len_tag_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_id) free(_in_id);
	if (_in_ts_out) free(_in_ts_out);
	if (_in_tag_out) free(_in_tag_out);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_ol_local_key(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_ol_local_key_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_ol_local_key_t* ms = SGX_CAST(ms_ecall_ol_local_key_t*, pms);
	ms_ecall_ol_local_key_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_ol_local_key_t), ms, sizeof(ms_ecall_ol_local_key_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_id = __in_ms.ms_id;
	size_t _len_id = 32;
	uint8_t* _in_id = NULL;
	const uint8_t* _tmp_records = __in_ms.ms_records;
	uint32_t _tmp_rec_len = __in_ms.ms_rec_len;
	size_t _len_records = _tmp_rec_len;
	uint8_t* _in_records = NULL;
	int64_t* _tmp_key_out = __in_ms.ms_key_out;
	size_t _len_key_out = sizeof(int64_t);
	int64_t* _in_key_out = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_id, _len_id);
	CHECK_UNIQUE_POINTER(_tmp_records, _len_records);
	CHECK_UNIQUE_POINTER(_tmp_key_out, _len_key_out);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_id != NULL && _len_id != 0) {
		if ( _len_id % sizeof(*_tmp_id) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_id = (uint8_t*)malloc(_len_id);
		if (_in_id == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_id, _len_id, _tmp_id, _len_id)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_records != NULL && _len_records != 0) {
		if ( _len_records % sizeof(*_tmp_records) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_records = (uint8_t*)malloc(_len_records);
		if (_in_records == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_records, _len_records, _tmp_records, _len_records)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_key_out != NULL && _len_key_out != 0) {
		if ( _len_key_out % sizeof(*_tmp_key_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_key_out = (int64_t*)malloc(_len_key_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_key_out, 0, _len_key_out);
	}
	_in_retval = ecall_ol_local_key((const uint8_t*)_in_id, (const uint8_t*)_in_records, _tmp_rec_len, _in_key_out);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_key_out) {
		if (memcpy_verw_s(_tmp_key_out, _len_key_out, _in_key_out, _len_key_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_id) free(_in_id);
	if (_in_records) free(_in_records);
	if (_in_key_out) free(_in_key_out);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_ol_seal_car(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_ol_seal_car_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_ol_seal_car_t* ms = SGX_CAST(ms_ecall_ol_seal_car_t*, pms);
	ms_ecall_ol_seal_car_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_ol_seal_car_t), ms, sizeof(ms_ecall_ol_seal_car_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_items = __in_ms.ms_items;
	uint32_t _tmp_items_len = __in_ms.ms_items_len;
	size_t _len_items = _tmp_items_len;
	uint8_t* _in_items = NULL;
	int64_t* _tmp_index_out = __in_ms.ms_index_out;
	size_t _len_index_out = sizeof(int64_t);
	int64_t* _in_index_out = NULL;
	int64_t* _tmp_frontier_out = __in_ms.ms_frontier_out;
	size_t _len_frontier_out = sizeof(int64_t);
	int64_t* _in_frontier_out = NULL;
	uint8_t* _tmp_car_hash_out = __in_ms.ms_car_hash_out;
	size_t _len_car_hash_out = 32;
	uint8_t* _in_car_hash_out = NULL;
	uint8_t* _tmp_tag_out = __in_ms.ms_tag_out;
	size_t _len_tag_out = 32;
	uint8_t* _in_tag_out = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_items, _len_items);
	CHECK_UNIQUE_POINTER(_tmp_index_out, _len_index_out);
	CHECK_UNIQUE_POINTER(_tmp_frontier_out, _len_frontier_out);
	CHECK_UNIQUE_POINTER(_tmp_car_hash_out, _len_car_hash_out);
	CHECK_UNIQUE_POINTER(_tmp_tag_out, _len_tag_out);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_items != NULL && _len_items != 0) {
		if ( _len_items % sizeof(*_tmp_items) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_items = (uint8_t*)malloc(_len_items);
		if (_in_items == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_items, _len_items, _tmp_items, _len_items)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_index_out != NULL && _len_index_out != 0) {
		if ( _len_index_out % sizeof(*_tmp_index_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_index_out = (int64_t*)malloc(_len_index_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_index_out, 0, _len_index_out);
	}
	if (_tmp_frontier_out != NULL && _len_frontier_out != 0) {
		if ( _len_frontier_out % sizeof(*_tmp_frontier_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_frontier_out = (int64_t*)malloc(_len_frontier_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_frontier_out, 0, _len_frontier_out);
	}
	if (_tmp_car_hash_out != NULL && _len_car_hash_out != 0) {
		if ( _len_car_hash_out % sizeof(*_tmp_car_hash_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_car_hash_out = (uint8_t*)malloc(_len_car_hash_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_car_hash_out, 0, _len_car_hash_out);
	}
	if (_tmp_tag_out != NULL && _len_tag_out != 0) {
		if ( _len_tag_out % sizeof(*_tmp_tag_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_tag_out = (uint8_t*)malloc(_len_tag_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_tag_out, 0, _len_tag_out);
	}
	_in_retval = ecall_ol_seal_car((const uint8_t*)_in_items, _tmp_items_len, _in_index_out, _in_frontier_out, _in_car_hash_out, _in_tag_out);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_index_out) {
		if (memcpy_verw_s(_tmp_index_out, _len_index_out, _in_index_out, _len_index_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_frontier_out) {
		if (memcpy_verw_s(_tmp_frontier_out, _len_frontier_out, _in_frontier_out, _len_frontier_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_car_hash_out) {
		if (memcpy_verw_s(_tmp_car_hash_out, _len_car_hash_out, _in_car_hash_out, _len_car_hash_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_tag_out) {
		if (memcpy_verw_s(_tmp_tag_out, _len_tag_out, _in_tag_out, _len_tag_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_items) free(_in_items);
	if (_in_index_out) free(_in_index_out);
	if (_in_frontier_out) free(_in_frontier_out);
	if (_in_car_hash_out) free(_in_car_hash_out);
	if (_in_tag_out) free(_in_tag_out);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_bof_place(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_bof_place_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_bof_place_t* ms = SGX_CAST(ms_ecall_bof_place_t*, pms);
	ms_ecall_bof_place_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_bof_place_t), ms, sizeof(ms_ecall_bof_place_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_id = __in_ms.ms_id;
	size_t _len_id = 32;
	uint8_t* _in_id = NULL;
	int64_t* _tmp_car_out = __in_ms.ms_car_out;
	size_t _len_car_out = sizeof(int64_t);
	int64_t* _in_car_out = NULL;
	uint8_t* _tmp_tag_out = __in_ms.ms_tag_out;
	size_t _len_tag_out = 32;
	uint8_t* _in_tag_out = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_id, _len_id);
	CHECK_UNIQUE_POINTER(_tmp_car_out, _len_car_out);
	CHECK_UNIQUE_POINTER(_tmp_tag_out, _len_tag_out);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_id != NULL && _len_id != 0) {
		if ( _len_id % sizeof(*_tmp_id) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_id = (uint8_t*)malloc(_len_id);
		if (_in_id == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_id, _len_id, _tmp_id, _len_id)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_car_out != NULL && _len_car_out != 0) {
		if ( _len_car_out % sizeof(*_tmp_car_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_car_out = (int64_t*)malloc(_len_car_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_car_out, 0, _len_car_out);
	}
	if (_tmp_tag_out != NULL && _len_tag_out != 0) {
		if ( _len_tag_out % sizeof(*_tmp_tag_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_tag_out = (uint8_t*)malloc(_len_tag_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_tag_out, 0, _len_tag_out);
	}
	_in_retval = ecall_bof_place((const uint8_t*)_in_id, _in_car_out, _in_tag_out);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_car_out) {
		if (memcpy_verw_s(_tmp_car_out, _len_car_out, _in_car_out, _len_car_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_tag_out) {
		if (memcpy_verw_s(_tmp_tag_out, _len_tag_out, _in_tag_out, _len_tag_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_id) free(_in_id);
	if (_in_car_out) free(_in_car_out);
	if (_in_tag_out) free(_in_tag_out);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_bof_record_announcements(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_bof_record_announcements_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_bof_record_announcements_t* ms = SGX_CAST(ms_ecall_bof_record_announcements_t*, pms);
	ms_ecall_bof_record_announcements_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_bof_record_announcements_t), ms, sizeof(ms_ecall_bof_record_announcements_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_id = __in_ms.ms_id;
	size_t _len_id = 32;
	uint8_t* _in_id = NULL;
	const uint8_t* _tmp_records = __in_ms.ms_records;
	uint32_t _tmp_rec_len = __in_ms.ms_rec_len;
	size_t _len_records = _tmp_rec_len;
	uint8_t* _in_records = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_id, _len_id);
	CHECK_UNIQUE_POINTER(_tmp_records, _len_records);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_id != NULL && _len_id != 0) {
		if ( _len_id % sizeof(*_tmp_id) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_id = (uint8_t*)malloc(_len_id);
		if (_in_id == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_id, _len_id, _tmp_id, _len_id)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_records != NULL && _len_records != 0) {
		if ( _len_records % sizeof(*_tmp_records) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_records = (uint8_t*)malloc(_len_records);
		if (_in_records == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_records, _len_records, _tmp_records, _len_records)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	_in_retval = ecall_bof_record_announcements((const uint8_t*)_in_id, (const uint8_t*)_in_records, _tmp_rec_len);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}

err:
	if (_in_id) free(_in_id);
	if (_in_records) free(_in_records);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_bof_seal_car(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_bof_seal_car_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_bof_seal_car_t* ms = SGX_CAST(ms_ecall_bof_seal_car_t*, pms);
	ms_ecall_bof_seal_car_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_bof_seal_car_t), ms, sizeof(ms_ecall_bof_seal_car_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_items_out = __in_ms.ms_items_out;
	uint32_t _tmp_items_cap = __in_ms.ms_items_cap;
	size_t _len_items_out = _tmp_items_cap;
	uint8_t* _in_items_out = NULL;
	uint32_t* _tmp_num_items_out = __in_ms.ms_num_items_out;
	size_t _len_num_items_out = sizeof(uint32_t);
	uint32_t* _in_num_items_out = NULL;
	int64_t* _tmp_index_out = __in_ms.ms_index_out;
	size_t _len_index_out = sizeof(int64_t);
	int64_t* _in_index_out = NULL;
	int64_t* _tmp_w_out = __in_ms.ms_w_out;
	size_t _len_w_out = sizeof(int64_t);
	int64_t* _in_w_out = NULL;
	uint8_t* _tmp_m_out = __in_ms.ms_m_out;
	uint32_t _tmp_m_cap = __in_ms.ms_m_cap;
	size_t _len_m_out = _tmp_m_cap;
	uint8_t* _in_m_out = NULL;
	uint8_t* _tmp_car_hash_out = __in_ms.ms_car_hash_out;
	size_t _len_car_hash_out = 32;
	uint8_t* _in_car_hash_out = NULL;
	uint8_t* _tmp_tag_out = __in_ms.ms_tag_out;
	size_t _len_tag_out = 32;
	uint8_t* _in_tag_out = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_items_out, _len_items_out);
	CHECK_UNIQUE_POINTER(_tmp_num_items_out, _len_num_items_out);
	CHECK_UNIQUE_POINTER(_tmp_index_out, _len_index_out);
	CHECK_UNIQUE_POINTER(_tmp_w_out, _len_w_out);
	CHECK_UNIQUE_POINTER(_tmp_m_out, _len_m_out);
	CHECK_UNIQUE_POINTER(_tmp_car_hash_out, _len_car_hash_out);
	CHECK_UNIQUE_POINTER(_tmp_tag_out, _len_tag_out);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_items_out != NULL && _len_items_out != 0) {
		if ( _len_items_out % sizeof(*_tmp_items_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_items_out = (uint8_t*)malloc(_len_items_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_items_out, 0, _len_items_out);
	}
	if (_tmp_num_items_out != NULL && _len_num_items_out != 0) {
		if ( _len_num_items_out % sizeof(*_tmp_num_items_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_num_items_out = (uint32_t*)malloc(_len_num_items_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_num_items_out, 0, _len_num_items_out);
	}
	if (_tmp_index_out != NULL && _len_index_out != 0) {
		if ( _len_index_out % sizeof(*_tmp_index_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_index_out = (int64_t*)malloc(_len_index_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_index_out, 0, _len_index_out);
	}
	if (_tmp_w_out != NULL && _len_w_out != 0) {
		if ( _len_w_out % sizeof(*_tmp_w_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_w_out = (int64_t*)malloc(_len_w_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_w_out, 0, _len_w_out);
	}
	if (_tmp_m_out != NULL && _len_m_out != 0) {
		if ( _len_m_out % sizeof(*_tmp_m_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_m_out = (uint8_t*)malloc(_len_m_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_m_out, 0, _len_m_out);
	}
	if (_tmp_car_hash_out != NULL && _len_car_hash_out != 0) {
		if ( _len_car_hash_out % sizeof(*_tmp_car_hash_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_car_hash_out = (uint8_t*)malloc(_len_car_hash_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_car_hash_out, 0, _len_car_hash_out);
	}
	if (_tmp_tag_out != NULL && _len_tag_out != 0) {
		if ( _len_tag_out % sizeof(*_tmp_tag_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_tag_out = (uint8_t*)malloc(_len_tag_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_tag_out, 0, _len_tag_out);
	}
	_in_retval = ecall_bof_seal_car(_in_items_out, _tmp_items_cap, _in_num_items_out, _in_index_out, _in_w_out, _in_m_out, _tmp_m_cap, _in_car_hash_out, _in_tag_out);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_items_out) {
		if (memcpy_verw_s(_tmp_items_out, _len_items_out, _in_items_out, _len_items_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_num_items_out) {
		if (memcpy_verw_s(_tmp_num_items_out, _len_num_items_out, _in_num_items_out, _len_num_items_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_index_out) {
		if (memcpy_verw_s(_tmp_index_out, _len_index_out, _in_index_out, _len_index_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_w_out) {
		if (memcpy_verw_s(_tmp_w_out, _len_w_out, _in_w_out, _len_w_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_m_out) {
		if (memcpy_verw_s(_tmp_m_out, _len_m_out, _in_m_out, _len_m_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_car_hash_out) {
		if (memcpy_verw_s(_tmp_car_hash_out, _len_car_hash_out, _in_car_hash_out, _len_car_hash_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_tag_out) {
		if (memcpy_verw_s(_tmp_tag_out, _len_tag_out, _in_tag_out, _len_tag_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_items_out) free(_in_items_out);
	if (_in_num_items_out) free(_in_num_items_out);
	if (_in_index_out) free(_in_index_out);
	if (_in_w_out) free(_in_w_out);
	if (_in_m_out) free(_in_m_out);
	if (_in_car_hash_out) free(_in_car_hash_out);
	if (_in_tag_out) free(_in_tag_out);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_heartbeat(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_heartbeat_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_heartbeat_t* ms = SGX_CAST(ms_ecall_heartbeat_t*, pms);
	ms_ecall_heartbeat_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_heartbeat_t), ms, sizeof(ms_ecall_heartbeat_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_id_out = __in_ms.ms_id_out;
	size_t _len_id_out = 32;
	uint8_t* _in_id_out = NULL;
	int64_t* _tmp_value_out = __in_ms.ms_value_out;
	size_t _len_value_out = sizeof(int64_t);
	int64_t* _in_value_out = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_id_out, _len_id_out);
	CHECK_UNIQUE_POINTER(_tmp_value_out, _len_value_out);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_id_out != NULL && _len_id_out != 0) {
		if ( _len_id_out % sizeof(*_tmp_id_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_id_out = (uint8_t*)malloc(_len_id_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_id_out, 0, _len_id_out);
	}
	if (_tmp_value_out != NULL && _len_value_out != 0) {
		if ( _len_value_out % sizeof(*_tmp_value_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_value_out = (int64_t*)malloc(_len_value_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_value_out, 0, _len_value_out);
	}
	_in_retval = ecall_heartbeat(_in_id_out, _in_value_out);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_id_out) {
		if (memcpy_verw_s(_tmp_id_out, _len_id_out, _in_id_out, _len_id_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_value_out) {
		if (memcpy_verw_s(_tmp_value_out, _len_value_out, _in_value_out, _len_value_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_id_out) free(_in_id_out);
	if (_in_value_out) free(_in_value_out);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_sign_block(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_sign_block_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_sign_block_t* ms = SGX_CAST(ms_ecall_sign_block_t*, pms);
	ms_ecall_sign_block_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_sign_block_t), ms, sizeof(ms_ecall_sign_block_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_digest = __in_ms.ms_digest;
	size_t _len_digest = 32;
	uint8_t* _in_digest = NULL;
	uint8_t* _tmp_tag_out = __in_ms.ms_tag_out;
	size_t _len_tag_out = 32;
	uint8_t* _in_tag_out = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_digest, _len_digest);
	CHECK_UNIQUE_POINTER(_tmp_tag_out, _len_tag_out);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_digest != NULL && _len_digest != 0) {
		if ( _len_digest % sizeof(*_tmp_digest) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_digest = (uint8_t*)malloc(_len_digest);
		if (_in_digest == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_digest, _len_digest, _tmp_digest, _len_digest)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_tag_out != NULL && _len_tag_out != 0) {
		if ( _len_tag_out % sizeof(*_tmp_tag_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_tag_out = (uint8_t*)malloc(_len_tag_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_tag_out, 0, _len_tag_out);
	}
	_in_retval = ecall_sign_block(__in_ms.ms_view, __in_ms.ms_height, (const uint8_t*)_in_digest, _in_tag_out);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_tag_out) {
		if (memcpy_verw_s(_tmp_tag_out, _len_tag_out, _in_tag_out, _len_tag_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_digest) free(_in_digest);
	if (_in_tag_out) free(_in_tag_out);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_verify_batch(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_verify_batch_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_verify_batch_t* ms = SGX_CAST(ms_ecall_verify_batch_t*, pms);
	ms_ecall_verify_batch_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_verify_batch_t), ms, sizeof(ms_ecall_verify_batch_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_records = __in_ms.ms_records;
	uint32_t _tmp_rec_len = __in_ms.ms_rec_len;
	size_t _len_records = _tmp_rec_len;
	uint8_t* _in_records = NULL;
	uint8_t* _tmp_ok_out = __in_ms.ms_ok_out;
	uint32_t _tmp_count = __in_ms.ms_count;
	size_t _len_ok_out = _tmp_count;
	uint8_t* _in_ok_out = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_records, _len_records);
	CHECK_UNIQUE_POINTER(_tmp_ok_out, _len_ok_out);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_records != NULL && _len_records != 0) {
		if ( _len_records % sizeof(*_tmp_records) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_records = (uint8_t*)malloc(_len_records);
		if (_in_records == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_records, _len_records, _tmp_records, _len_records)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_ok_out != NULL && _len_ok_out != 0) {
		if ( _len_ok_out % sizeof(*_tmp_ok_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_ok_out = (uint8_t*)malloc(_len_ok_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_ok_out, 0, _len_ok_out);
	}
	_in_retval = ecall_verify_batch((const uint8_t*)_in_records, _tmp_rec_len, _in_ok_out, _tmp_count);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_ok_out) {
		if (memcpy_verw_s(_tmp_ok_out, _len_ok_out, _in_ok_out, _len_ok_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_records) free(_in_records);
	if (_in_ok_out) free(_in_ok_out);
	return status;
}

SGX_EXTERNC const struct {
	size_t nr_ecall;
	struct {void* ecall_addr; uint8_t is_priv; uint8_t is_switchless;} ecall_table[10];
} g_ecall_table = {
	10,
	{
		{(void*)(uintptr_t)sgx_ecall_init, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_ol_attest, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_ol_local_key, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_ol_seal_car, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_bof_place, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_bof_record_announcements, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_bof_seal_car, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_heartbeat, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_sign_block, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_verify_batch, 0, 0},
	}
};

SGX_EXTERNC const struct {
	size_t nr_ocall;
	uint8_t entry_table[6][10];
} g_dyn_entry_table = {
	6,
	{
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
	}
};


sgx_status_t SGX_CDECL ocall_get_time(int64_t* t)
{
	sgx_status_t status = SGX_SUCCESS;
	size_t _len_t = sizeof(int64_t);

	ms_ocall_get_time_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_ocall_get_time_t);
	void *__tmp = NULL;

	void *__tmp_t = NULL;

	CHECK_ENCLAVE_POINTER(t, _len_t);

	if (ADD_ASSIGN_OVERFLOW(ocalloc_size, (t != NULL) ? _len_t : 0))
		return SGX_ERROR_INVALID_PARAMETER;

	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_ocall_get_time_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_ocall_get_time_t));
	ocalloc_size -= sizeof(ms_ocall_get_time_t);

	if (t != NULL) {
		if (memcpy_verw_s(&ms->ms_t, sizeof(int64_t*), &__tmp, sizeof(int64_t*))) {
			sgx_ocfree();
			return SGX_ERROR_UNEXPECTED;
		}
		__tmp_t = __tmp;
		if (_len_t % sizeof(*t) != 0) {
			sgx_ocfree();
			return SGX_ERROR_INVALID_PARAMETER;
		}
		memset_verw(__tmp_t, 0, _len_t);
		__tmp = (void *)((size_t)__tmp + _len_t);
		ocalloc_size -= _len_t;
	} else {
		ms->ms_t = NULL;
	}

	status = sgx_ocall(0, ms);

	if (status == SGX_SUCCESS) {
		if (t) {
			if (memcpy_s((void*)t, _len_t, __tmp_t, _len_t)) {
				sgx_ocfree();
				return SGX_ERROR_UNEXPECTED;
			}
		}
	}
	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_oc_cpuidex(int cpuinfo[4], int leaf, int subleaf)
{
	sgx_status_t status = SGX_SUCCESS;
	size_t _len_cpuinfo = 4 * sizeof(int);

	ms_sgx_oc_cpuidex_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_oc_cpuidex_t);
	void *__tmp = NULL;

	void *__tmp_cpuinfo = NULL;

	CHECK_ENCLAVE_POINTER(cpuinfo, _len_cpuinfo);

	if (ADD_ASSIGN_OVERFLOW(ocalloc_size, (cpuinfo != NULL) ? _len_cpuinfo : 0))
		return SGX_ERROR_INVALID_PARAMETER;

	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_oc_cpuidex_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_oc_cpuidex_t));
	ocalloc_size -= sizeof(ms_sgx_oc_cpuidex_t);

	if (cpuinfo != NULL) {
		if (memcpy_verw_s(&ms->ms_cpuinfo, sizeof(int*), &__tmp, sizeof(int*))) {
			sgx_ocfree();
			return SGX_ERROR_UNEXPECTED;
		}
		__tmp_cpuinfo = __tmp;
		if (_len_cpuinfo % sizeof(*cpuinfo) != 0) {
			sgx_ocfree();
			return SGX_ERROR_INVALID_PARAMETER;
		}
		memset_verw(__tmp_cpuinfo, 0, _len_cpuinfo);
		__tmp = (void *)((size_t)__tmp + _len_cpuinfo);
		ocalloc_size -= _len_cpuinfo;
	} else {
		ms->ms_cpuinfo = NULL;
	}

	if (memcpy_verw_s(&ms->ms_leaf, sizeof(ms->ms_leaf), &leaf, sizeof(leaf))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	if (memcpy_verw_s(&ms->ms_subleaf, sizeof(ms->ms_subleaf), &subleaf, sizeof(subleaf))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	status = sgx_ocall(1, ms);

	if (status == SGX_SUCCESS) {
		if (cpuinfo) {
			if (memcpy_s((void*)cpuinfo, _len_cpuinfo, __tmp_cpuinfo, _len_cpuinfo)) {
				sgx_ocfree();
				return SGX_ERROR_UNEXPECTED;
			}
		}
	}
	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_thread_wait_untrusted_event_ocall(int* retval, const void* self)
{
	sgx_status_t status = SGX_SUCCESS;

	ms_sgx_thread_wait_untrusted_event_ocall_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_thread_wait_untrusted_event_ocall_t);
	void *__tmp = NULL;


	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_thread_wait_untrusted_event_ocall_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_thread_wait_untrusted_event_ocall_t));
	ocalloc_size -= sizeof(ms_sgx_thread_wait_untrusted_event_ocall_t);

	if (memcpy_verw_s(&ms->ms_self, sizeof(ms->ms_self), &self, sizeof(self))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	status = sgx_ocall(2, ms);

	if (status == SGX_SUCCESS) {
		if (retval) {
			if (memcpy_s((void*)retval, sizeof(*retval), &ms->ms_retval, sizeof(ms->ms_retval))) {
				sgx_ocfree();
				return SGX_ERROR_UNEXPECTED;
			}
		}
	}
	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_thread_set_untrusted_event_ocall(int* retval, const void* waiter)
{
	sgx_status_t status = SGX_SUCCESS;

	ms_sgx_thread_set_untrusted_event_ocall_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_thread_set_untrusted_event_ocall_t);
	void *__tmp = NULL;


	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_thread_set_untrusted_event_ocall_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_thread_set_untrusted_event_ocall_t));
	ocalloc_size -= sizeof(ms_sgx_thread_set_untrusted_event_ocall_t);

	if (memcpy_verw_s(&ms->ms_waiter, sizeof(ms->ms_waiter), &waiter, sizeof(waiter))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	status = sgx_ocall(3, ms);

	if (status == SGX_SUCCESS) {
		if (retval) {
			if (memcpy_s((void*)retval, sizeof(*retval), &ms->ms_retval, sizeof(ms->ms_retval))) {
				sgx_ocfree();
				return SGX_ERROR_UNEXPECTED;
			}
		}
	}
	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_thread_setwait_untrusted_events_ocall(int* retval, const void* waiter, const void* self)
{
	sgx_status_t status = SGX_SUCCESS;

	ms_sgx_thread_setwait_untrusted_events_ocall_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_thread_setwait_untrusted_events_ocall_t);
	void *__tmp = NULL;


	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_thread_setwait_untrusted_events_ocall_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_thread_setwait_untrusted_events_ocall_t));
	ocalloc_size -= sizeof(ms_sgx_thread_setwait_untrusted_events_ocall_t);

	if (memcpy_verw_s(&ms->ms_waiter, sizeof(ms->ms_waiter), &waiter, sizeof(waiter))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	if (memcpy_verw_s(&ms->ms_self, sizeof(ms->ms_self), &self, sizeof(self))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	status = sgx_ocall(4, ms);

	if (status == SGX_SUCCESS) {
		if (retval) {
			if (memcpy_s((void*)retval, sizeof(*retval), &ms->ms_retval, sizeof(ms->ms_retval))) {
				sgx_ocfree();
				return SGX_ERROR_UNEXPECTED;
			}
		}
	}
	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_thread_set_multiple_untrusted_events_ocall(int* retval, const void** waiters, size_t total)
{
	sgx_status_t status = SGX_SUCCESS;
	size_t _len_waiters = total * sizeof(void*);

	ms_sgx_thread_set_multiple_untrusted_events_ocall_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_thread_set_multiple_untrusted_events_ocall_t);
	void *__tmp = NULL;


	CHECK_ENCLAVE_POINTER(waiters, _len_waiters);

	if (ADD_ASSIGN_OVERFLOW(ocalloc_size, (waiters != NULL) ? _len_waiters : 0))
		return SGX_ERROR_INVALID_PARAMETER;

	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_thread_set_multiple_untrusted_events_ocall_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_thread_set_multiple_untrusted_events_ocall_t));
	ocalloc_size -= sizeof(ms_sgx_thread_set_multiple_untrusted_events_ocall_t);

	if (waiters != NULL) {
		if (memcpy_verw_s(&ms->ms_waiters, sizeof(const void**), &__tmp, sizeof(const void**))) {
			sgx_ocfree();
			return SGX_ERROR_UNEXPECTED;
		}
		if (_len_waiters % sizeof(*waiters) != 0) {
			sgx_ocfree();
			return SGX_ERROR_INVALID_PARAMETER;
		}
		if (memcpy_verw_s(__tmp, ocalloc_size, waiters, _len_waiters)) {
			sgx_ocfree();
			return SGX_ERROR_UNEXPECTED;
		}
		__tmp = (void *)((size_t)__tmp + _len_waiters);
		ocalloc_size -= _len_waiters;
	} else {
		ms->ms_waiters = NULL;
	}

	if (memcpy_verw_s(&ms->ms_total, sizeof(ms->ms_total), &total, sizeof(total))) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}

	status = sgx_ocall(5, ms);

	if (status == SGX_SUCCESS) {
		if (retval) {
			if (memcpy_s((void*)retval, sizeof(*retval), &ms->ms_retval, sizeof(ms->ms_retval))) {
				sgx_ocfree();
				return SGX_ERROR_UNEXPECTED;
			}
		}
	}
	sgx_ocfree();
	return status;
}

