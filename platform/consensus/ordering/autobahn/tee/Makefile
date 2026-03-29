# Makefile for AutoBahn TEE Enclave (Intel SGX Simulation Mode)
#
# Build targets:
#   make            → build enclave + generate stubs + build libtee_host.a
#   make clean      → remove all generated/built files
#
# Prerequisites: Intel SGX SDK installed at /opt/intel/sgxsdk
# Usage (from project root):
#   cd platform/consensus/ordering/autobahn/tee && make
#   Then: bazel build //benchmark/protocols/autobahn:kv_server_performance

SGX_SDK    ?= /opt/intel/sgxsdk
SGX_MODE   := SIM      # Simulation mode — no hardware driver needed
SGX_ARCH   := x64
SGX_DEBUG  := 1

include $(SGX_SDK)/buildenv.mk

SGX_LIBRARY_PATH        := $(SGX_SDK)/lib64
SGX_TRUSTED_LIBRARY_PATH := $(SGX_SDK)/lib64
SGX_EDGER8R             := $(SGX_SDK)/bin/x64/sgx_edger8r
SGX_ENCLAVE_SIGNER      := $(SGX_SDK)/bin/x64/sgx_sign

ENCLAVE_DIR := enclave
GEN_DIR     := gen

# ---- Simulation runtime library names ----
Urts_Lib    := sgx_urts_sim
Trts_Lib    := sgx_trts_sim
Service_Lib := sgx_tservice_sim
Crypto_Lib  := sgx_tcrypto

# ---- Enclave build flags ----
Enclave_Include := \
    -I$(SGX_SDK)/include \
    -I$(SGX_SDK)/include/tlibc \
    -I$(SGX_SDK)/include/libcxx \
    -I$(GEN_DIR)

Enclave_CXX_Flags := \
    $(Enclave_Include) \
    -nostdinc -fvisibility=hidden -fpie \
    -ffunction-sections -fdata-sections \
    -fstack-protector-strong \
    -std=c++11 -O2

Enclave_Link_Flags := \
    -Wl,--no-undefined -nostdlib -nodefaultlibs -nostartfiles \
    -L$(SGX_TRUSTED_LIBRARY_PATH) \
    -Wl,--whole-archive -l$(Trts_Lib) -Wl,--no-whole-archive \
    -Wl,--start-group \
        -lsgx_tstdc -lsgx_tcxx -l$(Crypto_Lib) -l$(Service_Lib) \
    -Wl,--end-group \
    -Wl,-Bstatic -Wl,-Bsymbolic -Wl,--no-undefined \
    -Wl,-pie,-eenclave_entry -Wl,--export-dynamic \
    -Wl,--defsym,__ImageBase=0 -Wl,--gc-sections \
    -Wl,--version-script=$(ENCLAVE_DIR)/tee_enclave.lds

# ---- Host (untrusted) build flags ----
Host_Include := \
    -I$(SGX_SDK)/include \
    -I$(GEN_DIR) \
    -I../../../../..   # project root so #include "platform/..." works

Host_CXX_Flags := $(Host_Include) -fPIC -O2 -std=c++14 -Wall

# ---- Output names ----
Enclave_So        := tee_enclave.so
Signed_Enclave_So := tee_enclave.signed.so
Enclave_Config    := $(ENCLAVE_DIR)/tee_enclave.config.xml
Enclave_Test_Key  := $(ENCLAVE_DIR)/tee_enclave_private.pem

.PHONY: all gen_stubs enclave host clean

all: gen_stubs enclave host

# ---- Step 1: Generate trusted/untrusted stubs from EDL ----
gen_stubs: $(GEN_DIR)/tee_enclave_t.h $(GEN_DIR)/tee_enclave_u.h

$(GEN_DIR)/tee_enclave_t.h $(GEN_DIR)/tee_enclave_u.h: tee_enclave.edl
	@mkdir -p $(GEN_DIR)
	$(SGX_EDGER8R) --trusted   tee_enclave.edl --search-path $(SGX_SDK)/include \
	                                            --trusted-dir  $(GEN_DIR)
	$(SGX_EDGER8R) --untrusted tee_enclave.edl --search-path $(SGX_SDK)/include \
	                                            --untrusted-dir $(GEN_DIR)

# ---- Step 2: Build enclave .so ----
enclave: $(Signed_Enclave_So)

$(ENCLAVE_DIR)/tee_enclave.o: $(ENCLAVE_DIR)/tee_enclave.cpp $(GEN_DIR)/tee_enclave_t.h
	$(CXX) $(Enclave_CXX_Flags) -I$(GEN_DIR) -c $< -o $@

$(GEN_DIR)/tee_enclave_t.o: $(GEN_DIR)/tee_enclave_t.c $(GEN_DIR)/tee_enclave_t.h
	$(CC) -I$(SGX_SDK)/include -I$(SGX_SDK)/include/tlibc \
	      -nostdinc -fvisibility=hidden -fpie -fstack-protector-strong \
	      -ffunction-sections -fdata-sections -O2 \
	      -c $< -o $@

$(Enclave_So): $(ENCLAVE_DIR)/tee_enclave.o $(GEN_DIR)/tee_enclave_t.o
	$(CXX) $^ -o $@ $(Enclave_Link_Flags)

# Generate a test signing key if not present
$(Enclave_Test_Key):
	openssl genrsa -3 -out $@ 3072

$(Signed_Enclave_So): $(Enclave_So) $(Enclave_Test_Key) $(Enclave_Config)
	$(SGX_ENCLAVE_SIGNER) sign \
	    -key $(Enclave_Test_Key) \
	    -enclave $(Enclave_So) \
	    -out $@ \
	    -config $(Enclave_Config)
	@echo "[TEE] Signed enclave: $@"

# ---- Step 3: Build libtee_host.a ----
host: libtee_host.a

$(GEN_DIR)/tee_enclave_u.o: $(GEN_DIR)/tee_enclave_u.c $(GEN_DIR)/tee_enclave_u.h
	$(CC) -I$(SGX_SDK)/include -fPIC -O2 -c $< -o $@

tee_host.o: tee_host.cpp tee_host.h $(GEN_DIR)/tee_enclave_u.h
	$(CXX) $(Host_CXX_Flags) -c $< -o $@

libtee_host.a: $(GEN_DIR)/tee_enclave_u.o tee_host.o
	ar rcs $@ $^
	@echo "[TEE] Host library: $@"

clean:
	rm -f $(ENCLAVE_DIR)/*.o $(GEN_DIR)/*.o $(GEN_DIR)/*.h $(GEN_DIR)/*.c
	rm -f $(Enclave_So) $(Signed_Enclave_So) libtee_host.a tee_host.o
