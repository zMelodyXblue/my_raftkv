#!/usr/bin/env bash

# Shared settings for helper scripts under scripts/.
# Paths are relative to the my_raftkv project root unless absolute.

CLUSTER_NODES=(
  "0:config/node0.json"
  "1:config/node1.json"
  "2:config/node2.json"
)

CLUSTER_LOG_DIR="logs"
CLUSTER_PID_DIR="logs"

DEMO_CLIENT_CONFIG="config/node0.json"
DEMO_CLUSTER_WAIT_SEC=3
DEMO_FAIL_NODE_ID=0

DEMO_KEY_PRIMARY="demo_key"
DEMO_VALUE_PRIMARY="hello_raft"
DEMO_KEY_CITY="city"
DEMO_VALUE_CITY="Beijing"
DEMO_APPEND_KEY="greeting"
DEMO_APPEND_VALUE_1="Hello "
DEMO_APPEND_VALUE_2="World"
DEMO_KEY_AFTER_CRASH="after_crash"
DEMO_VALUE_AFTER_CRASH="still_works"

GATEWAY_CONFIG="config/node0.json"
GATEWAY_PORT=8080
GATEWAY_WEB_DIR="web"
GATEWAY_LOG_FILE="logs/gateway.log"
GATEWAY_PID_FILE="logs/gateway.pid"
GATEWAY_START_WAIT_SEC=1
