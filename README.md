# my_raftkv

基于 Raft 共识算法的分布式 KV 存储系统。支持 Leader 选举、日志复制、快照压缩、崩溃恢复、请求去重（基于结果缓存的 exactly-once 语义），以及可插拔存储引擎。

## 架构

```
                        ┌─────────────┐
                        │   KvClient  │  自动发现 Leader / 重试 / 去重
                        └──────┬──────┘
                               │ 传输层（gRPC / 自研 RPC / ...）
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
        ┌───────────┐   ┌───────────┐   ┌───────────┐
        │  Node 0   │   │  Node 1   │   │  Node 2   │
        │ KvService │   │ KvService │   │ KvService │
        │  KvStore  │   │  KvStore  │   │  KvStore  │
        │   Raft    │◄──►   Raft    │◄──►   Raft    │
        │ Persister │   │ Persister │   │ Persister │
        └───────────┘   └───────────┘   └───────────┘
```

**分层设计**：

| 层 | 职责 |
|----|------|
| KvClient | Leader 发现、自动重试、客户端去重 ID |
| NodeServer | 传输层服务端（gRPC 等），转发请求到 KvStore |
| KvStore | 状态机：Get/Put/Append、去重表、快照序列化 |
| Raft | 共识：选举、日志复制、commit 推进、快照截断 |
| Persister | 磁盘持久化（term、votedFor、日志、快照） |

## 特性

- **Leader 选举**：随机化超时 + RequestVote RPC
- **日志复制**：AppendEntries RPC + 快速回退优化
- **快照**：KvStore 驱动触发，Raft 截断日志，InstallSnapshot 同步落后节点
- **崩溃恢复**：持久化 Raft 状态 + 快照，重启后自动恢复
- **Exactly-once 语义**：client_id + request_id 去重，缓存每个 client 最后请求结果，重复请求直接返回缓存值；去重表随快照持久化，跨崩溃存活
- **可插拔存储引擎**：HashMap（默认）或自写 SkipList，通过配置一行切换
- **可插拔传输层**：RPC 后端通过抽象接口（`RaftPeerClient` / `KvServiceClient` / `NodeServer`）解耦，CMake 选项切换，支持 gRPC，预留自研 RPC 扩展
- **Redis 协议兼容**：通过 `raftkv_redis_proxy` 适配 RESP 协议，支持 redis-cli 和 redis-benchmark
- **Figure 8 保护**：只 commit 当前 term 的日志条目

## 依赖

| 依赖 | 版本 | 说明 |
|------|------|------|
| CMake | >= 3.16 | 构建系统 |
| C++ | C++14 | 编译标准 |
| gRPC | 1.16+ | RPC 框架 |
| Protobuf | 3.6+ | 序列化 |
| spdlog | - | 结构化日志 |
| GoogleTest | - | 单元/集成测试 |

Ubuntu/Debian 安装：

```bash
sudo apt install libgrpc++-dev libprotobuf-dev \
                 protobuf-compiler protobuf-compiler-grpc \
                 libspdlog-dev libgtest-dev \
                 cmake build-essential
```

## 构建

**一键构建**（编译 server、cli、bench、gateway）：

```bash
./scripts/cmake_build.sh
```

**仅编译 gateway**（前端/网关开发时使用，无需重编译 server）：

```bash
./scripts/build_gateway.sh
```

或手动：

```bash
cd my_raftkv
mkdir -p build && cd build
cmake ..
make raftkv_server raftkv_cli raftkv_bench raftkv_gateway -j$(nproc)
```

生成的可执行文件位于 `bin/`：

| 可执行文件 | 说明 |
|-----------|------|
| `raftkv_server` | Raft 节点服务端 |
| `raftkv_cli` | 命令行客户端 |
| `raftkv_bench` | 自研性能测试工具 |
| `raftkv_redis_proxy` | Redis 协议适配器（用于 redis-benchmark） |
| `raftkv_gateway` | REST 网关 + Web Dashboard 静态文件服务 |

## 快速开始

### Web Dashboard

```bash
./scripts/start_demo.sh
```

启动集群 + REST 网关。默认端口来自 `config/script_settings.sh` 中的 `GATEWAY_PORT`，浏览器打开对应地址即可使用 Web Dashboard：

- **KV 操作**：Get / Put / Append
- **集群状态**：实时查看各节点角色、term、日志进度

按 Ctrl+C 停止所有进程。

### 一键演示（CLI）

```bash
./scripts/demo.sh
```

自动执行：启动集群 -> 写入数据 -> 读取验证 -> kill 配置中的目标节点 -> 验证故障恢复 -> 写入新数据。

### 手动启动

**方式一：使用配置文件**

```bash
# 启动 3 节点
./bin/raftkv_server --config config/node0.json
./bin/raftkv_server --config config/node1.json
./bin/raftkv_server --config config/node2.json

# 读写
./bin/raftkv_cli --config config/node0.json put mykey myvalue
./bin/raftkv_cli --config config/node0.json get mykey
```

**方式二：命令行参数**

```bash
PEERS="127.0.0.1:50050,127.0.0.1:50051,127.0.0.1:50052"

./bin/raftkv_server --id 0 --peers $PEERS --data ./data/node0
./bin/raftkv_server --id 1 --peers $PEERS --data ./data/node1
./bin/raftkv_server --id 2 --peers $PEERS --data ./data/node2

./bin/raftkv_cli --peers $PEERS put mykey myvalue
./bin/raftkv_cli --peers $PEERS get mykey
```

**方式三：脚本管理（推荐本地开发）**

```bash
./scripts/start_cluster.sh    # 后台启动 3 节点
./scripts/stop_cluster.sh     # 停止所有节点
./scripts/demo.sh             # 运行 CLI 演示
./scripts/start_demo.sh       # 启动集群 + Gateway Demo
```

这些脚本现在**统一使用配置文件**，不再在脚本中硬编码节点地址、数据目录、Gateway 端口等设置：

- 节点启动配置：`config/node0.json`、`config/node1.json`、`config/node2.json`
- 脚本编排配置：`config/script_settings.sh`

例如：

- `start_cluster.sh` 会读取 `CLUSTER_NODES`
- `demo.sh` 会读取 `DEMO_CLIENT_CONFIG`、`DEMO_FAIL_NODE_ID`
- `start_demo.sh` 会读取 `GATEWAY_CONFIG`、`GATEWAY_PORT`、`GATEWAY_WEB_DIR`

## 配置

JSON 配置文件示例（`config/node0.json`）：

```json
{
  "node_id": 0,
  "peers": [
    "127.0.0.1:50050",
    "127.0.0.1:50051",
    "127.0.0.1:50052"
  ],
  "data_dir": "./data/node0",
  "engine_type": "skiplist",
  "raft": {
    "election_timeout_min_ms": 300,
    "election_timeout_max_ms": 500,
    "heartbeat_interval_ms": 30,
    "apply_interval_ms": 10,
    "rpc_timeout_ms": 500,
    "max_raft_state_bytes": 8388608
  }
}
```

`raft` 段可省略，使用默认值。命令行参数可覆盖配置文件：

```bash
raftkv_server --config config/node0.json --id 1  # 用 node0 的配置但覆盖 id 为 1
```

### 脚本配置

脚本相关参数统一放在 `config/script_settings.sh` 中，示例：

```bash
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

GATEWAY_CONFIG="config/node0.json"
GATEWAY_PORT=8080
GATEWAY_WEB_DIR="web"
```

推荐做法：

- 修改节点地址、数据目录、存储引擎：编辑 `config/node*.json`
- 修改 demo 等待时间、故障节点、Gateway 端口：编辑 `config/script_settings.sh`

### 配置项说明

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `node_id` | 0 | 节点编号 |
| `peers` | - | 所有节点地址 |
| `data_dir` | `./data` | 持久化目录 |
| `engine_type` | `hashmap` | `hashmap` 或 `skiplist` |
| `raft.election_timeout_min_ms` | 300 | 选举超时下限 |
| `raft.election_timeout_max_ms` | 500 | 选举超时上限 |
| `raft.heartbeat_interval_ms` | 30 | 心跳间隔 |
| `raft.apply_interval_ms` | 10 | applier 轮询间隔 |
| `raft.rpc_timeout_ms` | 500 | RPC 超时 |
| `raft.max_raft_state_bytes` | 8MB | 日志大小超过此值触发快照 |

## 多主机部署

三台云主机之间端口互通即可：

```bash
# 主机 10.0.1.10
raftkv_server --config node0.json   # peers 填真实内网 IP

# 主机 10.0.1.11
raftkv_server --config node1.json

# 主机 10.0.1.12
raftkv_server --config node2.json
```

## 性能测试

### 自研 benchmark

```bash
./bin/raftkv_bench --config config/node0.json \
                   --clients 4 --requests 10000 \
                   --value-size 256 --read-ratio 0.5
```

输出 QPS 和 P50/P95/P99 延迟（微秒）。理解业务语义（leader 重试、去重）。

### redis-benchmark（通过 Redis 协议适配器）

```bash
# 启动集群 + Redis 协议代理
./scripts/start_cluster.sh
./bin/raftkv_redis_proxy --peers 127.0.0.1:50050,127.0.0.1:50051,127.0.0.1:50052 --port 6379

# 标准压测（50 并发，4KB value）
redis-benchmark -p 6379 -t set,get -c 50 -n 10000 -d 4096

# 安静模式（只看 QPS）
redis-benchmark -p 6379 -t set,get -c 50 -n 10000 -q

# 保存 CSV
redis-benchmark -p 6379 -t set,get -c 50 -n 10000 --csv > bench_result.csv

# Pipeline 模式
redis-benchmark -p 6379 -t set,get -c 50 -n 50000 -P 16
```


## 测试

```bash
# 全部测试
cd build && ctest -V

# 单个测试（每个脚本自动编译对应 target）
./scripts/test_smoke.sh        # Phase 0: 基础类型
./scripts/test_persister.sh    # Phase 1: 持久化
./scripts/test_election.sh     # Phase 2: 选举
./scripts/test_replication.sh  # Phase 3: 日志复制
./scripts/test_kv.sh           # Phase 4: KV 操作
./scripts/test_snapshot.sh     # Phase 5: 快照
./scripts/test_stress.sh       # Phase 6A: 压力测试
./scripts/test_dedup.sh        # Phase 6A: 去重
./scripts/test_skiplist.sh     # Phase 6B: 跳表
```

## 项目结构

```
my_raftkv/
├── src/
│   ├── common/          # 抽象接口（RaftPeerClient, KvServiceClient, NodeServer,
│   │                    #   KvEngine）、DTO 定义、配置、工具类
│   ├── raft/            # Raft 共识核心（选举、复制、快照、持久化）
│   ├── kv_store/        # KV 状态机（apply、去重、快照）
│   ├── rpc/             # 传输层后端
│   │   └── grpc/        #   gRPC 实现（可替换为自研 RPC 等）
│   ├── client/          # KvClient 客户端库（传输无关）
│   ├── proto/           # protobuf 生成代码（proto_msg + proto_grpc）
│   ├── gateway/         # REST 网关 + Web Dashboard
│   ├── redis_proxy/     # Redis 协议适配器（RESP → KvClient）
│   ├── main.cpp         # 服务端入口
│   ├── cli.cpp          # CLI 客户端
│   └── benchmark.cpp    # 自研性能测试工具
├── proto/               # .proto 定义文件
├── tests/               # 测试文件（Phase 0-6B）
├── config/              # JSON 配置文件模板
├── web/                 # Web Dashboard 静态文件
├── scripts/             # 部署和测试脚本
└── CMakeLists.txt       # RAFTKV_RPC_BACKEND 选择传输后端
```

## 设计亮点

1. **传输层解耦**：核心层（Raft / KvStore）零传输依赖，通过 `RaftPeerClient` / `KvServiceClient` / `NodeServer` 三个抽象接口隔离，CMake 选项一键切换后端
2. **proto 库拆分**：`proto_msg`（纯序列化）与 `proto_grpc`（gRPC 桩代码）分离，非 gRPC 后端无需链接 gRPC
3. **wait_channel 无竞争创建**：`start()` 与 wait_channel 创建在同一临界区内，防止通知丢失
4. **Figure 8 保护**：只 commit 当前 term 日志，防止已提交日志被覆盖
5. **快照分层**：KvStore 驱动（知道状态大小），Raft 截断（不接触业务数据）
6. **去重结果缓存**：每个 client 最后请求的结果被缓存，重复请求返回相同结果（含 Get），去重表随快照持久化跨崩溃存活
7. **锁外 push 避免死锁**：在 Raft 互斥锁外 push apply_channel，防止 ABBA 死锁

