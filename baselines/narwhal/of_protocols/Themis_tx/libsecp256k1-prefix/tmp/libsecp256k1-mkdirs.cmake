# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/var/scratch/vrobotin/narwhal/of_protocols/Themis_tx/secp256k1"
  "/var/scratch/vrobotin/narwhal/of_protocols/Themis_tx/libsecp256k1-prefix/src/libsecp256k1-build"
  "/var/scratch/vrobotin/narwhal/of_protocols/Themis_tx/libsecp256k1-prefix"
  "/var/scratch/vrobotin/narwhal/of_protocols/Themis_tx/libsecp256k1-prefix/tmp"
  "/var/scratch/vrobotin/narwhal/of_protocols/Themis_tx/libsecp256k1-prefix/src/libsecp256k1-stamp"
  "/var/scratch/vrobotin/narwhal/of_protocols/Themis_tx/libsecp256k1-prefix/src"
  "/var/scratch/vrobotin/narwhal/of_protocols/Themis_tx/libsecp256k1-prefix/src/libsecp256k1-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/var/scratch/vrobotin/narwhal/of_protocols/Themis_tx/libsecp256k1-prefix/src/libsecp256k1-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/var/scratch/vrobotin/narwhal/of_protocols/Themis_tx/libsecp256k1-prefix/src/libsecp256k1-stamp${cfgdir}") # cfgdir has leading slash
endif()
