# my_raftkv

基于 Raft 共识算法的分布式 KV 存储系统。支持 Leader 选举、日志复制、快照压缩、崩溃恢复、请求去重（基于结果缓存的 exactly-once 语义），以及可插拔存储引擎。

## 架构

```
                        ┌─────────────┐
                        │   KvClient  │  自动发现 Leader / 重试 / 去重
                        └──────┬──────┘
                               │ gRPC
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
| KvService | gRPC 服务端，转发请求到 KvStore |
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

生成的可执行文件位于 `build/bin/`：

| 可执行文件 | 说明 |
|-----------|------|
| `raftkv_server` | Raft 节点服务端 |
| `raftkv_cli` | 命令行客户端 |
| `raftkv_bench` | 性能测试工具 |
| `raftkv_gateway` | REST 网关 + Web Dashboard 静态文件服务 |

## 快速开始

### Web Dashboard

```bash
./scripts/start_demo.sh
```

启动 3 节点集群 + REST 网关，浏览器打开 http://localhost:8080 即可使用 Web Dashboard：

- **KV 操作**：Get / Put / Append
- **集群状态**：实时查看各节点角色、term、日志进度

按 Ctrl+C 停止所有进程。

### 一键演示（CLI）

```bash
./scripts/demo.sh
```

自动执行：启动 3 节点集群 -> 写入数据 -> 读取验证 -> kill 节点 -> 验证故障恢复 -> 写入新数据。

### 手动启动

**方式一：使用配置文件**

```bash
# 启动 3 节点
./build/bin/raftkv_server --config config/node0.json
./build/bin/raftkv_server --config config/node1.json
./build/bin/raftkv_server --config config/node2.json

# 读写
./build/bin/raftkv_cli --config config/node0.json put mykey myvalue
./build/bin/raftkv_cli --config config/node0.json get mykey
```

**方式二：命令行参数**

```bash
PEERS="127.0.0.1:50050,127.0.0.1:50051,127.0.0.1:50052"

./build/bin/raftkv_server --id 0 --peers $PEERS --data ./data/node0
./build/bin/raftkv_server --id 1 --peers $PEERS --data ./data/node1
./build/bin/raftkv_server --id 2 --peers $PEERS --data ./data/node2

./build/bin/raftkv_cli --peers $PEERS put mykey myvalue
./build/bin/raftkv_cli --peers $PEERS get mykey
```

**方式三：脚本管理**

```bash
./scripts/start_cluster.sh    # 后台启动 3 节点
./scripts/stop_cluster.sh     # 停止所有节点
```

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

```bash
./build/bin/raftkv_bench --config config/node0.json \
                         --clients 4 \
                         --requests 10000 \
                         --value-size 256 \
                         --read-ratio 0.5
```

输出 QPS 和 P50/P95/P99 延迟（微秒）。

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
│   ├── common/          # 配置、类型、队列、存储引擎接口
│   ├── raft/            # Raft 共识核心（选举、复制、快照）
│   ├── kv_store/        # KV 状态机（apply、去重、快照）
│   ├── rpc/             # gRPC 服务层
│   ├── client/          # 客户端库
│   ├── proto/           # protobuf 生成代码
│   ├── gateway/         # REST 网关（httplib + gRPC 转发）
│   ├── main.cpp         # 服务端入口
│   ├── cli.cpp          # CLI 客户端
│   └── benchmark.cpp    # 性能测试工具
├── proto/               # .proto 定义文件
├── tests/               # 测试文件（Phase 0-6B）
├── config/              # JSON 配置文件模板
├── web/                 # Web Dashboard 静态文件
├── scripts/             # 部署和测试脚本
└── CMakeLists.txt
```

## 设计亮点

1. **wait_channel 无竞争创建**：`start()` 与 wait_channel 创建在同一临界区内，防止通知丢失
2. **Figure 8 保护**：只 commit 当前 term 日志，防止已提交日志被覆盖
3. **快照分层**：KvStore 驱动（知道状态大小），Raft 截断（不接触业务数据）
4. **去重结果缓存**：每个 client 最后请求的结果被缓存，重复请求返回相同结果（含 Get），去重表随快照持久化跨崩溃存活
5. **锁外 push 避免死锁**：在 Raft 互斥锁外 push apply_channel，防止 ABBA 死锁
