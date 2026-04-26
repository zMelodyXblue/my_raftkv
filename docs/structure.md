# 项目架构

```
my_raftkv/src/                                                                                                                    
├── common/           抽象接口 + DTO + 工具
│   ├── types.h           DTO（AppendEntriesArgs, KvOp, ApplyMsg...）
│   ├── raft_peer.h       RaftPeerClient 抽象（Raft 出站通信）
│   ├── kv_service_client.h  KvServiceClient 抽象（KV 客户端通信）
│   ├── node_server.h     NodeServer 抽象（服务端生命周期）
│   ├── kv_engine.h       KvEngine 抽象（存储引擎）
│   ├── config.h          ServerConfig / RaftConfig
│   └── thread_safe_queue.h  条件变量阻塞队列
├── raft/             Raft 共识核心（零传输依赖）
│   ├── raft.h/cpp        选举、复制、快照、applier 
│   └── persister.h/cpp   持久化（virtual，可被 WAL 子类覆盖）
├── kv_store/         状态机
│   └── kv_store.h/cpp    Get/Put/Append、去重、快照
├── rpc/              传输层后端
│   └── grpc/             gRPC 实现（通过 RAFTKV_RPC_BACKEND 选择）
├── client/           KvClient（传输无关，持有 KvServiceClient 接口）
├── redis_proxy/      Redis 协议适配器（RESP → KvClient）
├── gateway/          REST 网关 + Web Dashboard
└── main.cpp / cli.cpp / benchmark.cpp
```

**依赖方向**（严格单向）：
```
应用层(main/cli/bench/gateway/redis_proxy) 
  → 服务层(client, rpc/grpc)
    → 核心层(raft, kv_store)
      → 基础层(common)
```

核心层（raft, kv_store）不依赖 gRPC，只依赖 `proto_msg`（纯 protobuf 序列化）。
