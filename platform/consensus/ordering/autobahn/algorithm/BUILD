package(default_visibility = ["//platform/consensus/ordering/autobahn:__subpackages__"])

cc_library(
    name = "ol_ordering",
    srcs = ["ol_ordering.cpp"],
    hdrs = ["ol_ordering.h"],
    deps = ["//common:comm"],
)

cc_library(
    name = "bof_ordering",
    srcs = ["bof_ordering.cpp"],
    hdrs = ["bof_ordering.h"],
    deps = ["//common:comm"],
)

cc_test(
    name = "ordering_test",
    srcs = ["ordering_test.cpp"],
    deps = [
        ":bof_ordering",
        ":ol_ordering",
        "//common/test:test_main",
    ],
)

cc_library(
    name = "ranking",
    srcs = ["ranking.cpp"],
    hdrs = ["ranking.h"],
    deps = [
        "//common:comm",
    ],
)

cc_library(
    name = "autobahn",
    srcs = ["autobahn.cpp"],
    hdrs = ["autobahn.h"],
    linkopts = [
        "-L/var/scratch/vrobotin/sgxsdk/lib64",
        "-lsgx_urts_sim",
        "-lssl",
        "-lcrypto",
        "-lpthread",
    ],
    deps = [
        ":bof_ordering",
        ":ol_ordering",
        "//platform/statistic:stats",
        "//common:comm",
        "//common/crypto:signature_verifier",
        "//platform/consensus/ordering/common/algorithm:protocol_base",
        "//platform/consensus/ordering/autobahn/proto:proposal_cc_proto",
        "//platform/common/queue:lock_free_queue",
        "//platform/consensus/ordering/autobahn/tee:tee_host",
        "//platform/networkstrate:traffic",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
    ],
)
