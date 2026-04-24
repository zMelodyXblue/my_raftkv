#!/bin/bash
set -e
cd "$(dirname "$0")/../build"
cmake --build . --target test_stress -j$(nproc)
ctest -R stress -V
