---
title: "raft-java System Architecture"
author: "Felix Berger"
date: "June 2026"
geometry: margin=2.5cm
fontsize: 11pt
header-includes:
  - \usepackage{float}
  - \floatplacement{figure}{H}
---

# 1. Overview

**raft-java** is a from-scratch Java 17 implementation of the Raft consensus algorithm,
following Ongaro & Ousterhout's *"In Search of an Understandable Consensus Algorithm"*
and the PhD dissertation *"Consensus: Bridging Theory and Practice"*.

The project implements:

- **Core Raft** (§5): leader election, log replication, safety rules
- **Cluster reconfiguration** (§6): one-server-at-a-time membership changes
- **Log compaction** (§7): independent snapshotting with chunked InstallSnapshot
- **Client interaction** (§8): no-op entry, leader redirection
- **PreVote** (§4.2.3 / §9.6): prevents partitioned servers from disrupting the cluster
- **Performance optimizations** (§10.2): parallel leader disk writes, batching, pipelining
- **Copy-on-write snapshotting** (§5.1): async background compaction

\newpage

# 2. System Architecture

![System Architecture Overview](component.png){width=100%}

Each Raft node runs as a single JVM process. The **RaftNode** class owns the
consensus algorithm. It delegates durable storage to **RaftStorage** (backed by
Berkeley DB JE), state machine operations to **StateMachine**, and peer
communication to the **Transport** abstraction layer.

The transport layer is pluggable: a **gRPC** implementation (default) uses HTTP/2
with protobuf encoding, while a **Netty** implementation provides a lightweight
TCP transport with custom length-delimited framing.

Prometheus metrics are exposed via Micrometer counters, timers, and gauges on a
configurable HTTP endpoint.

\newpage

# 3. Class Diagram

![Core Class Relationships](class.png){width=100%}

Key design principles:

- **RaftNode** implements `RaftRpcHandler`, making it transport-agnostic
- All dependencies are injected (Spring IoC in production, manual wiring in tests)
- `RaftStorage` separates durable state from the algorithm
- `RaftTransport` / `RaftTransportFactory` abstract the network layer
- `RaftMetrics` instruments all significant state transitions

\newpage

# 4. Leader Election with PreVote

![Leader Election Sequence](election.png){width=85%}

The election process has two phases:

1. **PreVote (§4.2.3)**: The candidate checks with a majority that they would
   grant a vote AND haven't heard from a valid leader recently. This prevents
   a partitioned server from incrementing its term and disrupting the cluster
   when it rejoins. The term is *not* incremented during PreVote.

2. **Real Election (§5.2)**: Only after PreVote succeeds does the candidate
   increment its term, vote for itself, and send `RequestVote` RPCs. On
   receiving a majority, it becomes leader and immediately appends a no-op
   entry (§8).

Single-node clusters skip PreVote and elect immediately.

\newpage

# 5. Log Replication Pipeline

![Replication with Parallel Disk Writes and Pipelining](replication.png){width=85%}

Three optimizations from §10.2 reduce latency:

1. **Parallel leader disk writes (§10.2.1)**: The leader writes entries with
   `COMMIT_WRITE_NO_SYNC` and replicates to followers immediately. A background
   thread calls `flushLog(true)` to fsync. The leader tracks its own durable
   progress via `leaderDiskMatchIndex` and only counts itself in the commit
   majority when the fsync is confirmed.

2. **Batching (§10.2.2)**: Each `AppendEntries` RPC carries up to 1 MB of
   entries, amortizing per-RPC overhead under high load.

3. **Pipelining (§10.2.2)**: After sending entries, the leader optimistically
   advances `nextIndex` so the next heartbeat tick can send subsequent entries
   without waiting for the previous ACK. A per-peer inflight counter (max 2)
   prevents unbounded pipelining. On failure, `nextIndex` reverts to the last
   confirmed `matchIndex + 1`.

\newpage

# 6. Snapshot Transfer

![Chunked Snapshot Transfer](snapshot.png){width=85%}

Snapshots are handled in two parts:

**Taking snapshots (§5.1 COW)**: Automatic snapshots capture the state machine
bytes under the Raft lock (fast, in-memory), then persist and compact the log
on a background thread. This avoids blocking replication and elections during
disk I/O. A staleness check prevents a background snapshot from overwriting a
more recent `InstallSnapshot`.

**Transferring snapshots (§7, Figure 13)**: The leader packs the state machine
data and cluster configuration into a single byte stream
(`[4B smLen][smData][cfgData]`) and sends it in fixed-size chunks (default 1 MB).
One chunk is sent per heartbeat cycle. The follower buffers incoming chunks and
applies the reassembled snapshot when the final chunk arrives (`done=true`).

\newpage

# 7. Transport Layer

The transport abstraction decouples RaftNode from any specific network library:

| Interface | Purpose |
|-----------|---------|
| `RaftTransport` | Outbound peer connection (requestVote, appendEntries, installSnapshot, preVote) |
| `RaftTransportFactory` | Creates outbound connections from an address string |
| `RaftTransportServer` | Inbound server that routes RPCs to `RaftRpcHandler` |
| `RaftRpcHandler` | Handler interface for incoming RPCs (implemented by RaftNode) |

**gRPC implementation** (default): Uses HTTP/2 with protobuf encoding.
`GrpcTransport` wraps `RaftServiceGrpc.RaftServiceFutureStub` and converts
`ListenableFuture` to `CompletableFuture`. `GrpcTransportServer` embeds a
`RaftServiceAdapter` that delegates to `RaftRpcHandler`.

**Netty implementation**: Custom TCP protocol with length-delimited framing:

```
+----------+------+-------+---------+
| len (4B) | type | reqId | payload |
+----------+------+-------+---------+
```

Request IDs correlate responses to pending `CompletableFuture`s. Message types
1-8 cover the four RPC pairs (RequestVote, AppendEntries, InstallSnapshot,
PreVote). A shared `NioEventLoopGroup` serves all outbound connections.

# 8. Metrics

All significant Raft events are instrumented via Micrometer:

| Metric | Type | Description |
|--------|------|-------------|
| `raft.election.started` | Counter | Elections initiated |
| `raft.election.won` | Counter | Elections won |
| `raft.vote.granted` | Counter | Votes granted to candidates |
| `raft.stepdown` | Counter | Step-downs to follower |
| `raft.entry.applied` | Counter | Entries applied to state machine |
| `raft.replication.sent` | Counter | Entries sent to followers |
| `raft.replication.success` | Counter | Successful AppendEntries responses |
| `raft.replication.failure` | Counter | Failed AppendEntries (log mismatch) |
| `raft.snapshot.taken` | Counter | Snapshots taken |
| `raft.snapshot.installed` | Counter | Snapshots installed from leader |
| `raft.snapshot.chunk.sent` | Counter | Snapshot chunks sent |
| `raft.snapshot.chunk.received` | Counter | Snapshot chunks received |
| `raft.client.rejected` | Counter | Client submissions rejected (not leader) |
| `raft.rpc.append.entries` | Timer | AppendEntries RPC handling time |
| `raft.rpc.request.vote` | Timer | RequestVote RPC handling time |
| `raft.rpc.install.snapshot` | Timer | InstallSnapshot RPC handling time |
| `raft.client.submit` | Timer | Client submit-to-commit time |
| `raft.node.term` | Gauge | Current Raft term |
| `raft.node.commit.index` | Gauge | Highest committed log index |
| `raft.node.last.applied` | Gauge | Last applied log index |
| `raft.node.role` | Gauge | Current role (0=follower, 1=candidate, 2=leader) |
| `raft.cluster.size` | Gauge | Number of nodes in configuration |
