#!/bin/bash
set -e
cd "$(dirname "$0")/.."
mkdir -p build
cd build
cmake .. -DPIN_SYSTEM_PROTOBUF=ON
cmake --build . --target raftkv_gateway -j$(nproc)
