#!/bin/bash
set -e
cd "$(dirname "$0")/../build"
cmake --build . --target test_snapshot -j$(nproc)
ctest -R snapshot -V
