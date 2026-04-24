#!/bin/bash
set -e
cd "$(dirname "$0")/../build"
cmake --build . --target test_smoke -j$(nproc)
ctest -R smoke -V
