# fbtberger-raft

A from-scratch Java 17 implementation of the Raft consensus algorithm, following
Ongaro & Ousterhout's *"In Search of an Understandable Consensus Algorithm"* and
the PhD dissertation *"Consensus: Bridging Theory and Practice"*.

RPCs are defined with Protocol Buffers and transported over a pluggable transport
layer (gRPC, Netty TCP, or Hadoop RPC); persistent state is stored in Berkeley DB
Java Edition with synchronous durability. Logging uses SLF4J/Logback; observability
is provided via Micrometer/Prometheus metrics.

For a detailed system architecture with UML diagrams, see
[docs/architecture.pdf](docs/architecture.pdf).
For a quick API reference, see the
[developer cheatsheet](docs/cheatsheet.pdf).

## What is implemented

- **Leader election** with randomized timeouts and the election-restriction safety
  rule (§5.2, §5.4.1)
- **PreVote + Leader Stickiness** (§4.2.3 / §9.6): prevents partitioned servers
  from disrupting the cluster; `hasLeaderStickiness()` denies votes when a valid
  leader has been heard from recently
- **Leadership transfer** (§3.10): graceful handoff to a target server via
  `TimeoutNow` RPC, with automatic abort on timeout
- **Log replication** via `AppendEntries`, including consistency check, conflicting
  entry truncation, and the "only commit entries from the current term" safety
  rule (§5.3, §5.4.2)
- **Performance optimizations** (§10.2):
  - Parallel leader disk writes (§10.2.1): leader replicates before its own fsync
  - Batching (§10.2.2): up to 1 MB per AppendEntries RPC
  - Pipelining (§10.2.2): optimistic nextIndex advancement, max 2 in-flight RPCs
- **No-op entry** committed by every new leader (§8)
- **Cluster reconfiguration** (§6):
  - Single-step: `addServer()` / `removeServer()` for one-at-a-time changes
  - Joint consensus: `setConfiguration()` for arbitrary multi-server changes via
    a two-phase C_old,new → C_new protocol with separate majorities
  - Non-voting learners (§4.2.1): `addLearner()` / `promoteLearner()` /
    `removeLearner()` — a new server catches up as a learner (replicated to, but
    excluded from every majority) and is promoted to a voter only once caught up,
    so adding capacity never opens an availability gap
  - §4 errata fix (config changes rejected until leader's no-op commits)
- **Log compaction / snapshotting** (§7):
  - Chunked InstallSnapshot (Figure 13) with configurable chunk size
  - Copy-on-write async snapshotting (§5.1): `prepareCowSnapshot()` captures a
    lightweight reference under the lock; serialization and disk I/O run on a
    background thread without blocking the Raft lock
  - Independent per-server compaction with configurable threshold
- **Transport abstraction layer**: pluggable gRPC, Netty TCP, and Hadoop RPC
  implementations, with per-RPC configurable timeouts and TLS/mTLS support
- **Prometheus metrics + JMX**: counters, timers, and gauges published to both
  Prometheus and JMX; `RaftNodeMXBean` for JConsole/VisualVM
- **Health checks**: `/health` (liveness) and `/ready` (readiness) HTTP endpoints
- **SLF4J/Logback** structured logging
- **Durable storage** behind a `RaftStorage` interface:
  - `BerkeleyDbStorage` — Berkeley DB JE with synchronous commit
  - `WalStorage` — segmented append-only WAL with CRC32 frame checksums,
    configurable max segment size (default 64 MB), and crash recovery
  - `InMemoryStorage` — non-durable, for tests/demos
- **Linearizable reads**:
  - ReadIndex protocol: `readIndex()` confirms leadership with a majority
    heartbeat round before allowing reads
  - Lease-based: `leaseRead()` serves reads immediately when the leader's lease
    is valid (majority acked within election timeout); falls back to ReadIndex
- **Spring IoC** wiring with `@ConditionalOnMissingBean` for overridable components
- **Docker** deployment configuration
- **JaCoCo** test coverage reporting

## Project layout

```
fbtberger-raft/
├── build.gradle.kts
├── LICENSE                         # Apache 2.0
├── config/
│   ├── Dockerfile                  # container image for the raft node
│   ├── node1.properties            # example 3-node cluster config
│   ├── node2.properties
│   └── node3.properties
├── docs/
│   ├── architecture.pdf            # system architecture with UML diagrams
│   ├── cheatsheet.pdf              # developer quick reference
│   └── *.puml / *.png              # PlantUML diagram sources and renders
└── src/
    ├── main/
    │   ├── proto/
    │   │   ├── raft.proto              # RequestVote, AppendEntries, InstallSnapshot,
    │   │   │                           # PreVote, TimeoutNow
    │   │   └── client.proto            # Submit, AddServer, RemoveServer
    │   ├── resources/
    │   │   └── logback-raftserver.xml  # logging for the standalone server only
    │   └── java/com/fbtberger/raft/
    │       ├── RaftNode.java               # core algorithm
    │       ├── RaftConfig.java             # node configuration from .properties
    │       ├── RaftMetrics.java            # Micrometer instrumentation
    │       ├── ServerRole.java             # FOLLOWER / CANDIDATE / LEADER
    │       ├── RaftStorage.java            # durable state interface
    │       ├── BerkeleyDbStorage.java      # Berkeley DB JE implementation
    │       ├── WalStorage.java             # segmented WAL with CRC32 checksums
    │       ├── InMemoryStorage.java        # non-durable test implementation
    │       ├── StateMachine.java           # state machine interface (+ COW snapshot)
    │       ├── KeyValueStateMachine.java   # demo key-value store
    │       ├── RaftClientGrpcService.java  # client-facing gRPC adapter
    │       ├── RaftNodeMXBean.java         # JMX management interface
    │       ├── RaftNodeMBean.java          # JMX MXBean implementation
    │       ├── HealthCheck.java            # liveness + readiness checks
    │       ├── RaftNodeConfiguration.java  # Spring IoC wiring
    │       ├── RaftServer.java             # main() + interactive CLI
    │       ├── transport/
    │       │   ├── RaftTransport.java           # outbound peer interface
    │       │   ├── RaftTransportFactory.java     # connection factory interface
    │       │   ├── RaftTransportServer.java      # inbound server interface
    │       │   ├── RaftRpcHandler.java           # RPC handler interface
    │       │   ├── RpcTimeouts.java              # per-RPC timeout configuration
    │       │   ├── TimeoutTransport.java         # timeout decorator
    │       │   ├── TlsConfig.java                # TLS/mTLS configuration
    │       │   ├── GrpcTransport.java            # gRPC implementation
    │       │   ├── GrpcTransportFactory.java
    │       │   ├── GrpcTransportServer.java
    │       │   ├── NettyTransport.java           # Netty TCP implementation
    │       │   ├── NettyTransportFactory.java
    │       │   ├── NettyTransportServer.java
    │       │   ├── NettyProtocol.java            # wire format constants
    │       │   ├── HadoopTransport.java          # Hadoop RPC implementation
    │       │   ├── HadoopTransportFactory.java
    │       │   ├── HadoopTransportServer.java
    │       │   └── HadoopRaftProtocol.java       # Hadoop protocol interface
    │       └── client/
    │           ├── RaftClient.java               # generic client with leader discovery
    │           ├── KeyValueClient.java            # key-value convenience wrapper
    │           ├── RaftClientException.java        # client error type
    │           └── RaftClientDemo.java            # standalone client process
    └── test/java/com/fbtberger/raft/
        ├── RaftNodeTest.java                   # unit tests (single-node + multi-node)
        ├── ThreeNodeClusterTest.java           # integration tests (3-node gRPC in-process)
        ├── MultiTransportClusterTest.java      # 3-node cluster across all transports
        ├── ChaosTest.java                      # partition simulation (4 scenarios)
        ├── TlsAndTimeoutsTest.java             # TLS/mTLS integration + timeout config
        ├── HealthCheckTest.java                # liveness + readiness tests
        ├── RaftClientGrpcServiceTest.java      # client RPC tests
        ├── RaftNodeConfigurationTest.java      # Spring wiring tests
        ├── RaftStorageContract.java            # ONE invariant set for every RaftStorage impl
        ├── InMemoryStorageContractTest.java    #   -> in-memory (durability tests skipped)
        ├── BerkeleyDbStorageContractTest.java  #   -> Berkeley DB, real environment on disk
        ├── WalStorageContractTest.java         #   -> segmented write-ahead log
        ├── WalStorageSegmentTest.java          # WAL-only: rollover, truncate across segments
        ├── KeyValueStateMachineTest.java       # state machine tests
        ├── RaftBenchmark.java                  # JMH performance benchmarks
        └── transport/
            ├── NettyTransportTest.java         # Netty round-trip tests
            └── HadoopTransportTest.java        # Hadoop round-trip tests
```

## Configuration

All settings are loaded from a `.properties` file:

```properties
# Required
node.id=node1
node.port=9091
data.dir=/var/raft/node1
peer.node1=host1:9091
peer.node2=host2:9092
peer.node3=host3:9093

# Optional (defaults shown)
snapshot.threshold=100          # entries before auto-snapshot
snapshot.chunk.size=1048576     # InstallSnapshot chunk size (bytes)
metrics.port=0                  # Prometheus endpoint (0 = disabled)

# Per-RPC timeouts (ms)
rpc.timeout.request.vote.ms=1000
rpc.timeout.append.entries.ms=2000
rpc.timeout.install.snapshot.ms=30000
rpc.timeout.pre.vote.ms=1000

# TLS (optional)
tls.enabled=false
tls.cert.path=/path/to/cert.pem
tls.key.path=/path/to/key.pem
tls.ca.path=/path/to/ca.pem
tls.mtls.enabled=false

# Election switches -- defaults shown. See "Election switches" below before
# changing any of these: the non-default value of each one is a known defect.
raft.prevote.quorum-latch=true
raft.election.boot-delay-factor=6
raft.prevote.leader-stickiness=true
```

### Election switches

Three keys can put a fixed election defect back into a running node, so a talk can show the
failure instead of describing it. Each defaults to the fixed behaviour, and a `-D` system
property of the same name overrides the file — one artifact and one properties file can
therefore serve a run with the defect and a run without it.

| Key | Default | The non-default value restores |
|---|---|---|
| `raft.prevote.quorum-latch` | `true` | **Issue #3.** The PreVote quorum handler fires once per grant instead of once per round: two peers granting the same round run two full elections, no step-down in between. |
| `raft.election.boot-delay-factor` | `6` | **Issue #2, requester side.** `1` puts a freshly started node on the ordinary 150–300 ms timeout, so it campaigns before a live leader's transport has reconnected to it (the measured gap was 498–802 ms). |
| `raft.prevote.leader-stickiness` | `true` | **Issue #2, responder side.** A LEADER stops counting as having leader contact and grants the (pre-)vote that unseats it. With three voters this grant *is* the challenger's quorum. |

Issue #2 needs both of its switches to reproduce: the boot delay decides whether the restarting
node campaigns, stickiness decides whether it wins. Lower the boot delay alone and the node
campaigns and is refused by everyone — visible in the log, no leader change.

A node whose switches are not all default logs that at WARN on startup, so a demo trace can
still be read months later.

## Building

```
./gradlew build
```

This produces a shadow JAR at `build/libs/fbtberger-raft-1.0.0-all.jar`.

## Running a demo cluster

Open three terminals, one per node:

```
java -jar build/libs/fbtberger-raft-1.0.0-all.jar config/node1.properties
java -jar build/libs/fbtberger-raft-1.0.0-all.jar config/node2.properties
java -jar build/libs/fbtberger-raft-1.0.0-all.jar config/node3.properties
```

Each node runs an interactive CLI:

```
SET foo bar      # submit through Raft, wait for commit
GET foo          # read from local state machine (use readIndex() for linearizable)
ADD n4 host:port        # add a voting member (§6, leader only)
REMOVE n4               # remove a voting member (§6, leader only)
ADDLEARNER n4 host:port # add a non-voting learner (§4.2.1, leader only)
PROMOTE n4             # promote a caught-up learner to a voting member (leader only)
REMOVELEARNER n4       # remove a non-voting learner (leader only)
SNAPSHOT         # force immediate log compaction (§7)
STATUS           # print role, leader, configuration, snapshot boundary
quit             # shut down
```

## Running with Docker

```
./gradlew shadowJar
docker build -f config/Dockerfile -t fbtberger-raft .
```

## Metrics & JMX

When `metrics.port` is configured, the following HTTP endpoints are exposed:

```
curl http://localhost:10091/metrics   # Prometheus scrape
curl http://localhost:10091/health    # liveness (always 200)
curl http://localhost:10091/ready     # readiness (200 if leader known, 503 otherwise)
```

All Micrometer metrics are also published to JMX. A `RaftNodeMXBean` under
`com.fbtberger.raft:type=RaftNode` exposes role, term, cluster members, and
operations (triggerSnapshot, transferLeadership) for JConsole/VisualVM.

See [docs/architecture.pdf](docs/architecture.pdf) for the full metrics table.

## Testing

```
./gradlew test                    # run all tests
./gradlew jacocoTestReport        # generate coverage report
open build/reports/jacoco/test/html/index.html
```

## What is *not* implemented

- **Client request de-duplication** (§8): retried commands may be applied twice.
  A production system would track per-client serial numbers.

## License

[Apache License 2.0](LICENSE)
