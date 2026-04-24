#!/bin/bash
set -e
cd "$(dirname "$0")/../build"
cmake --build . --target test_dedup -j$(nproc)
ctest -R dedup -V
