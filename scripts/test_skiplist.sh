#!/bin/bash
set -e
cd "$(dirname "$0")/../build"
cmake --build . --target test_skiplist -j$(nproc)
ctest -R skiplist -V
