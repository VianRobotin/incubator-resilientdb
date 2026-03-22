package(default_visibility = ["//visibility:private"])

cc_library(
    name = "consensus",
    srcs = ["consensus.cpp"],
    hdrs = ["consensus.h"],
    visibility = [
        "//visibility:public",
    ],
    deps = [
        "//common/crypto:signature_verifier",
        "//common/utils",
        "//platform/consensus/ordering/common/framework:consensus",
        "//platform/consensus/ordering/autobahn/algorithm:autobahn",
        "//proto/kv:kv_cc_proto",
    ],
)

