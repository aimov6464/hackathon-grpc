# hackathon-grpc
This project implements a high-performance balance management server using gRPC and RocksDB. It is designed to process financial transactions in real-time with high throughput (TPS).

## Key Features
Balance Updates: Handles deposits and withdrawals while checking for duplicate transactions.
Duplicate Transaction Detection: Implements optimized duplicate transaction checks using RocksDB.
High-Performance Data Storage: Uses RocksDB for efficient key-value storage with optimized compaction and caching strategies.
Batch Processing: Aggregates multiple balance update requests in a single atomic batch operation, reducing disk I/O overhead.
Multi-Shard Support: Each instance operates on a specific shard to distribute load efficiently.
Concurrency Optimizations: Configured for high parallelism with RocksDB’s pipelined writes and direct I/O optimizations.
Performance Optimizations
Write Batch Processing to reduce I/O overhead.
Optimized RocksDB settings for faster read/write operations.
Efficient memory management with caching strategies.
This project is ideal for real-time financial applications that require low-latency balance updates with high transaction throughput.
## Experiment
### Compression
gzip, zstd, brotili

### gRPC HTTP/2 flow control
HTTP/2 flow control window sizes to reduce throttling:
* Server: .initialFlowControlWindowSize(int size)
* Client: .flowControlWindow(int size)

Netty?
https://github.com/netty/netty/issues/10193

### gRPC message size
* .maxInboundMessageSize(10 * 1024 * 1024) // 10 MB

### gRPC connections
Try two or more gRPC channels (connections)

### proto-buf tuning
* TCP_NODELAY and SO_SNDBUF options for lower-latency network transmission
* Packed encoding (works only for primitive types like uint32 and uint64)
  repeated AccountRequest request = 1 [packed = true];

### Netty native transports
* IO_uring: For the highest performance on Linux systems with kernel 5.1+.
* Epoll: For Linux systems where IO_uring is unavailable.
* KQueue: For macOS systems.

### Serialization format
FlatBuffers, Cap’n Proto, Apache Fury
https://github.com/eishay/jvm-serializers/wiki

### Networking Stack
* Use an optimized networking stack like IO_uring (Linux) or Netty native transport for better CPU utilization in network I/O.
* Configure system-wide send/receive buffers:
  sysctl -w net.core.rmem_max=16777216
  sysctl -w net.core.wmem_max=16777216
  ulimit -n 65535

### Network Interface Tuning
Increasing MTU (Maximum Transmission Unit): Using jumbo frames can reduce overhead for large data transfers.
