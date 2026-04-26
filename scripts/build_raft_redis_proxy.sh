#!/bin/bash
set -e
cd "$(dirname "$0")/../build"
cmake --build . --target raftkv_redis_proxy -j$(nproc)
