#!/bin/bash
set -e
cd "$(dirname "$0")/../build"
cmake --build . --target test_kv -j$(nproc)
ctest -R kv -V
