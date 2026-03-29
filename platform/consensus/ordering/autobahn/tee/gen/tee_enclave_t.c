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

static sgx_status_t SGX_CDECL sgx_ecall_init_tee(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_init_tee_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_init_tee_t* ms = SGX_CAST(ms_ecall_init_tee_t*, pms);
	ms_ecall_init_tee_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_init_tee_t), ms, sizeof(ms_ecall_init_tee_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_pubkey_out = __in_ms.ms_pubkey_out;
	size_t _len_pubkey_out = 64;
	uint8_t* _in_pubkey_out = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_pubkey_out, _len_pubkey_out);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_pubkey_out != NULL && _len_pubkey_out != 0) {
		if ( _len_pubkey_out % sizeof(*_tmp_pubkey_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_pubkey_out = (uint8_t*)malloc(_len_pubkey_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_pubkey_out, 0, _len_pubkey_out);
	}
	_in_retval = ecall_init_tee(_in_pubkey_out);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_pubkey_out) {
		if (memcpy_verw_s(_tmp_pubkey_out, _len_pubkey_out, _in_pubkey_out, _len_pubkey_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_pubkey_out) free(_in_pubkey_out);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_timestamp_txn(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_timestamp_txn_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_timestamp_txn_t* ms = SGX_CAST(ms_ecall_timestamp_txn_t*, pms);
	ms_ecall_timestamp_txn_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_timestamp_txn_t), ms, sizeof(ms_ecall_timestamp_txn_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_hash = __in_ms.ms_hash;
	size_t _len_hash = 32;
	uint8_t* _in_hash = NULL;
	int64_t* _tmp_timestamp_out = __in_ms.ms_timestamp_out;
	size_t _len_timestamp_out = sizeof(int64_t);
	int64_t* _in_timestamp_out = NULL;
	uint8_t* _tmp_sig_out = __in_ms.ms_sig_out;
	size_t _len_sig_out = 64;
	uint8_t* _in_sig_out = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_hash, _len_hash);
	CHECK_UNIQUE_POINTER(_tmp_timestamp_out, _len_timestamp_out);
	CHECK_UNIQUE_POINTER(_tmp_sig_out, _len_sig_out);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_hash != NULL && _len_hash != 0) {
		if ( _len_hash % sizeof(*_tmp_hash) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_hash = (uint8_t*)malloc(_len_hash);
		if (_in_hash == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_hash, _len_hash, _tmp_hash, _len_hash)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_timestamp_out != NULL && _len_timestamp_out != 0) {
		if ( _len_timestamp_out % sizeof(*_tmp_timestamp_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_timestamp_out = (int64_t*)malloc(_len_timestamp_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_timestamp_out, 0, _len_timestamp_out);
	}
	if (_tmp_sig_out != NULL && _len_sig_out != 0) {
		if ( _len_sig_out % sizeof(*_tmp_sig_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_sig_out = (uint8_t*)malloc(_len_sig_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_sig_out, 0, _len_sig_out);
	}
	_in_retval = ecall_timestamp_txn((const uint8_t*)_in_hash, _in_timestamp_out, _in_sig_out);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_timestamp_out) {
		if (memcpy_verw_s(_tmp_timestamp_out, _len_timestamp_out, _in_timestamp_out, _len_timestamp_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_sig_out) {
		if (memcpy_verw_s(_tmp_sig_out, _len_sig_out, _in_sig_out, _len_sig_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_hash) free(_in_hash);
	if (_in_timestamp_out) free(_in_timestamp_out);
	if (_in_sig_out) free(_in_sig_out);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_assign_sequence_number(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_assign_sequence_number_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_assign_sequence_number_t* ms = SGX_CAST(ms_ecall_assign_sequence_number_t*, pms);
	ms_ecall_assign_sequence_number_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_assign_sequence_number_t), ms, sizeof(ms_ecall_assign_sequence_number_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_hash = __in_ms.ms_hash;
	size_t _len_hash = 32;
	uint8_t* _in_hash = NULL;
	int64_t* _tmp_seq_out = __in_ms.ms_seq_out;
	size_t _len_seq_out = sizeof(int64_t);
	int64_t* _in_seq_out = NULL;
	uint8_t* _tmp_sig_out = __in_ms.ms_sig_out;
	size_t _len_sig_out = 64;
	uint8_t* _in_sig_out = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_hash, _len_hash);
	CHECK_UNIQUE_POINTER(_tmp_seq_out, _len_seq_out);
	CHECK_UNIQUE_POINTER(_tmp_sig_out, _len_sig_out);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_hash != NULL && _len_hash != 0) {
		if ( _len_hash % sizeof(*_tmp_hash) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_hash = (uint8_t*)malloc(_len_hash);
		if (_in_hash == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_hash, _len_hash, _tmp_hash, _len_hash)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_seq_out != NULL && _len_seq_out != 0) {
		if ( _len_seq_out % sizeof(*_tmp_seq_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_seq_out = (int64_t*)malloc(_len_seq_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_seq_out, 0, _len_seq_out);
	}
	if (_tmp_sig_out != NULL && _len_sig_out != 0) {
		if ( _len_sig_out % sizeof(*_tmp_sig_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_sig_out = (uint8_t*)malloc(_len_sig_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_sig_out, 0, _len_sig_out);
	}
	_in_retval = ecall_assign_sequence_number((const uint8_t*)_in_hash, _in_seq_out, _in_sig_out);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_seq_out) {
		if (memcpy_verw_s(_tmp_seq_out, _len_seq_out, _in_seq_out, _len_seq_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}
	if (_in_sig_out) {
		if (memcpy_verw_s(_tmp_sig_out, _len_sig_out, _in_sig_out, _len_sig_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_hash) free(_in_hash);
	if (_in_seq_out) free(_in_seq_out);
	if (_in_sig_out) free(_in_sig_out);
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_sign_bytes(void* pms)
{
	CHECK_REF_POINTER(pms, sizeof(ms_ecall_sign_bytes_t));
	//
	// fence after pointer checks
	//
	sgx_lfence();
	ms_ecall_sign_bytes_t* ms = SGX_CAST(ms_ecall_sign_bytes_t*, pms);
	ms_ecall_sign_bytes_t __in_ms;
	if (memcpy_s(&__in_ms, sizeof(ms_ecall_sign_bytes_t), ms, sizeof(ms_ecall_sign_bytes_t))) {
		return SGX_ERROR_UNEXPECTED;
	}
	sgx_status_t status = SGX_SUCCESS;
	const uint8_t* _tmp_data = __in_ms.ms_data;
	uint32_t _tmp_data_len = __in_ms.ms_data_len;
	size_t _len_data = _tmp_data_len;
	uint8_t* _in_data = NULL;
	uint8_t* _tmp_sig_out = __in_ms.ms_sig_out;
	size_t _len_sig_out = 64;
	uint8_t* _in_sig_out = NULL;
	sgx_status_t _in_retval;

	CHECK_UNIQUE_POINTER(_tmp_data, _len_data);
	CHECK_UNIQUE_POINTER(_tmp_sig_out, _len_sig_out);

	//
	// fence after pointer checks
	//
	sgx_lfence();

	if (_tmp_data != NULL && _len_data != 0) {
		if ( _len_data % sizeof(*_tmp_data) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		_in_data = (uint8_t*)malloc(_len_data);
		if (_in_data == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		if (memcpy_s(_in_data, _len_data, _tmp_data, _len_data)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}

	}
	if (_tmp_sig_out != NULL && _len_sig_out != 0) {
		if ( _len_sig_out % sizeof(*_tmp_sig_out) != 0)
		{
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
		if ((_in_sig_out = (uint8_t*)malloc(_len_sig_out)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_sig_out, 0, _len_sig_out);
	}
	_in_retval = ecall_sign_bytes((const uint8_t*)_in_data, _tmp_data_len, _in_sig_out);
	if (memcpy_verw_s(&ms->ms_retval, sizeof(ms->ms_retval), &_in_retval, sizeof(_in_retval))) {
		status = SGX_ERROR_UNEXPECTED;
		goto err;
	}
	if (_in_sig_out) {
		if (memcpy_verw_s(_tmp_sig_out, _len_sig_out, _in_sig_out, _len_sig_out)) {
			status = SGX_ERROR_UNEXPECTED;
			goto err;
		}
	}

err:
	if (_in_data) free(_in_data);
	if (_in_sig_out) free(_in_sig_out);
	return status;
}

SGX_EXTERNC const struct {
	size_t nr_ecall;
	struct {void* ecall_addr; uint8_t is_priv; uint8_t is_switchless;} ecall_table[4];
} g_ecall_table = {
	4,
	{
		{(void*)(uintptr_t)sgx_ecall_init_tee, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_timestamp_txn, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_assign_sequence_number, 0, 0},
		{(void*)(uintptr_t)sgx_ecall_sign_bytes, 0, 0},
	}
};

SGX_EXTERNC const struct {
	size_t nr_ocall;
	uint8_t entry_table[6][4];
} g_dyn_entry_table = {
	6,
	{
		{0, 0, 0, 0, },
		{0, 0, 0, 0, },
		{0, 0, 0, 0, },
		{0, 0, 0, 0, },
		{0, 0, 0, 0, },
		{0, 0, 0, 0, },
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

