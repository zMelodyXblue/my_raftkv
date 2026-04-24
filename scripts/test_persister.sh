#!/bin/bash
set -e
cd "$(dirname "$0")/../build"
cmake --build . --target test_persister -j$(nproc)
ctest -R persister -V
