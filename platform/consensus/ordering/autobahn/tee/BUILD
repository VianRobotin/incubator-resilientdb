package(default_visibility = ["//platform/consensus/ordering/autobahn:__subpackages__"])

# libtee_host.a is built externally by the Makefile in this directory.
# Run `make` here before `bazel build` to regenerate it.
cc_import(
    name = "tee_host_prebuilt",
    static_library = "libtee_host.a",
    hdrs = ["tee_host.h"],
    # The generated untrusted header is included by tee_host.h transitively
    # via the .cpp file; expose gen/ for any direct includers if needed.
    includes = ["."],
)

# Convenience alias used by algorithm/BUILD
cc_library(
    name = "tee_host",
    hdrs = ["tee_host.h"],
    deps = [":tee_host_prebuilt"],
    linkopts = [
        "-L/var/scratch/vrobotin/sgxsdk/lib64",
        "-lsgx_urts_sim",
        "-lssl",
        "-lcrypto",
        "-lpthread",
    ],
)
