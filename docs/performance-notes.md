# Performance Notes & Experiments

This document captures **performance tuning directions and experiments** explored for the
high-throughput balance processing service.

Not all options are enabled by default. Results depend on workload characteristics,
hardware configuration, and deployment environment.

---

## Measurement Scope

Performance was evaluated across **distinct execution paths** to isolate bottlenecks:

- **gRPC transport / in-memory execution path**
  - request parsing
  - validation
  - in-memory processing
  - response serialization
- **Full persistent execution path**
  - request processing
  - state mutation
  - persistence to storage
  - response generation

This separation helps distinguish **CPU / networking limits** from **storage-bound throughput**.

---

## gRPC / HTTP2

### Flow Control

HTTP/2 flow control window tuning can reduce throttling under high concurrency.

Relevant configuration points:

- **Server**
  - `NettyServerBuilder#initialFlowControlWindowSize(int)`
- **Client**
  - `NettyChannelBuilder#flowControlWindow(int)`

Adjusting window sizes can reduce backpressure effects when large numbers of concurrent
streams are active.

Reference:
- https://github.com/netty/netty/issues/10193

---

### Message Size

For larger payloads, inbound message limits may need adjustment:

```java
.maxInboundMessageSize(10 * 1024 * 1024) // 10 MB
```

---

### Connections / Channels

- Using multiple gRPC channels per client was evaluated to reduce head-of-line blocking.
- Improves parallelism under high fan-out or high concurrency workloads.
- Trade-off: increased connection and resource management complexity.

---

## Serialization

### Protobuf Tuning

General considerations:

- Minimize message size and field count.
- Avoid deeply nested structures when possible.
- Prefer flat message layouts for hot paths.

Packed encoding can reduce payload size for repeated primitive fields:

```proto
repeated uint64 ids = 1 [packed = true];
```

---

### Alternative Serialization Formats (Exploration)

Alternative formats evaluated or considered for comparison:

- **FlatBuffers**
- **Cap’n Proto**
- **Apache Fury**

Benchmark reference for JVM serialization performance:
- https://github.com/eishay/jvm-serializers/wiki

---

## Transport / Netty

### Native Transports

Native transports were explored to improve CPU efficiency and reduce syscall overhead:

- **Linux**
  - `io_uring` (kernel 5.1+)
  - `epoll`
- **macOS**
  - `kqueue`

---

### Socket Options

Socket-level tuning options evaluated depending on workload characteristics:

- `TCP_NODELAY`
- `SO_SNDBUF`

---

## Compression

Compression strategies were evaluated based on payload size and CPU budget:

- `gzip`
- `zstd`
- `brotli`

---

## Storage Layer

### Storage Engines

The persistent execution path was evaluated using different storage engines:

- **RocksDB**
- **Speedb** (RocksDB-compatible engine)

---

## OS / Network Tuning (Linux)

### System Limits

```bash
sysctl -w net.core.rmem_max=16777216
sysctl -w net.core.wmem_max=16777216
ulimit -n 65535
```

---

### MTU / Jumbo Frames

- Jumbo frames can reduce per-packet overhead for large payloads.
- Requires end-to-end network support.

---

## Notes

The primary goal of the system is **predictable latency and correctness under extreme load**,
rather than maximizing raw throughput at the expense of reliability.
