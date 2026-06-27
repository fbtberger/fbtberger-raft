# raft-java

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
- **PreVote** (§4.2.3 / §9.6): prevents partitioned servers from disrupting the
  cluster by inflating their term on rejoin
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
- **Cluster reconfiguration** (§6): one-server-at-a-time membership changes, with
  the §4 errata fix (config changes rejected until leader's no-op commits)
- **Log compaction / snapshotting** (§7):
  - Chunked InstallSnapshot (Figure 13) with configurable chunk size
  - Copy-on-write async snapshotting (§5.1): background disk I/O
  - Independent per-server compaction with configurable threshold
- **Transport abstraction layer**: pluggable gRPC, Netty TCP, and Hadoop RPC
  implementations, with per-RPC configurable timeouts
- **Prometheus metrics**: counters, timers, and gauges for all Raft events
- **SLF4J/Logback** structured logging
- **Durable storage** behind a `RaftStorage` interface with `BerkeleyDbStorage`
  (synchronous-commit) and `InMemoryStorage` (tests/demos)
- **Spring IoC** wiring with `@ConditionalOnMissingBean` for overridable components
- **Docker** deployment configuration
- **JaCoCo** test coverage reporting

## Project layout

```
raft-java/
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
    │   │   └── logback.xml             # logging configuration
    │   └── java/com/fbtberger/raft/
    │       ├── RaftNode.java               # core algorithm
    │       ├── RaftConfig.java             # node configuration from .properties
    │       ├── RaftMetrics.java            # Micrometer instrumentation
    │       ├── ServerRole.java             # FOLLOWER / CANDIDATE / LEADER
    │       ├── RaftStorage.java            # durable state interface
    │       ├── BerkeleyDbStorage.java      # Berkeley DB JE implementation
    │       ├── InMemoryStorage.java        # non-durable test implementation
    │       ├── StateMachine.java           # state machine interface
    │       ├── KeyValueStateMachine.java   # demo key-value store
    │       ├── RaftGrpcService.java        # metrics-wrapped gRPC adapter
    │       ├── RaftClientGrpcService.java  # client-facing gRPC adapter
    │       ├── RaftNodeConfiguration.java  # Spring IoC wiring
    │       ├── RaftServer.java             # main() + interactive CLI
    │       ├── transport/
    │       │   ├── RaftTransport.java           # outbound peer interface
    │       │   ├── RaftTransportFactory.java     # connection factory interface
    │       │   ├── RaftTransportServer.java      # inbound server interface
    │       │   ├── RaftRpcHandler.java           # RPC handler interface
    │       │   ├── RpcTimeouts.java              # per-RPC timeout configuration
    │       │   ├── TimeoutTransport.java         # timeout decorator
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
    │           └── RaftClientDemo.java            # standalone client process
    └── test/java/com/fbtberger/raft/
        ├── RaftNodeTest.java                   # unit tests (single-node + multi-node)
        ├── ThreeNodeClusterTest.java           # integration tests (3-node in-process)
        ├── RaftClientGrpcServiceTest.java      # client RPC tests
        ├── RaftNodeConfigurationTest.java      # Spring wiring tests
        ├── InMemoryStorageTest.java            # storage contract tests
        └── KeyValueStateMachineTest.java       # state machine tests
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
```

## Building

```
./gradlew build
```

This produces a shadow JAR at `build/libs/raft-java-1.0.0-all.jar`.

## Running a demo cluster

Open three terminals, one per node:

```
java -jar build/libs/raft-java-1.0.0-all.jar config/node1.properties
java -jar build/libs/raft-java-1.0.0-all.jar config/node2.properties
java -jar build/libs/raft-java-1.0.0-all.jar config/node3.properties
```

Each node runs an interactive CLI:

```
SET foo bar      # submit through Raft, wait for commit
GET foo          # read from local state machine (non-linearizable)
ADD n4 host:port # add a voting member (§6, leader only)
REMOVE n4        # remove a voting member (§6, leader only)
SNAPSHOT         # force immediate log compaction (§7)
STATUS           # print role, leader, configuration, snapshot boundary
quit             # shut down
```

## Running with Docker

```
./gradlew shadowJar
docker build -f config/Dockerfile -t raft-java .
```

## Metrics

When `metrics.port` is configured, a Prometheus-compatible `/metrics` endpoint
is exposed:

```
curl http://localhost:10091/metrics
```

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
