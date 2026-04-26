#!/bin/bash
set -e
cd "$(dirname "$0")/../build"
cmake --build . --target test_replication -j$(nproc)
ctest -R kv_skiplist -V
