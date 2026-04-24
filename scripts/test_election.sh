#!/bin/bash
set -e
cd "$(dirname "$0")/../build"
cmake --build . --target test_election -j$(nproc)
ctest -R election -V
