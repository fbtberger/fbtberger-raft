# raft-java

A from-scratch Java 17 implementation of the Raft consensus algorithm, following
Ongaro & Ousterhout's *"In Search of an Understandable Consensus Algorithm"* and
the PhD dissertation *"Consensus: Bridging Theory and Practice"*.

RPCs are defined with Protocol Buffers and transported over a pluggable transport
layer (gRPC or Netty TCP); persistent state is stored in Berkeley DB Java Edition
with synchronous durability. Observability is provided via Micrometer/Prometheus
metrics.

For a detailed system architecture with UML diagrams, see
[docs/architecture.pdf](docs/architecture.pdf).
For a quick API reference, see the
[developer cheatsheet](docs/cheatsheet.pdf).

## What is implemented

- **Leader election** with randomized timeouts and the election-restriction safety
  rule (§5.2, §5.4.1)
- **PreVote** (§4.2.3 / §9.6): prevents partitioned servers from disrupting the
  cluster by inflating their term on rejoin
- **Log replication** via `AppendEntries`, including consistency check, conflicting
  entry truncation, and the "only commit entries from the current term" safety
  rule (§5.3, §5.4.2)
- **Performance optimizations** (§10.2):
  - Parallel leader disk writes (§10.2.1): leader replicates before its own fsync
  - Batching (§10.2.2): up to 1 MB per AppendEntries RPC
  - Pipelining (§10.2.2): optimistic nextIndex advancement, max 2 in-flight RPCs
- **No-op entry** committed by every new leader (§8)
- **Cluster reconfiguration** (§6): one-server-at-a-time membership changes
- **Log compaction / snapshotting** (§7):
  - Chunked InstallSnapshot (Figure 13) with configurable chunk size
  - Copy-on-write async snapshotting (§5.1): background disk I/O
  - Independent per-server compaction with configurable threshold
- **Transport abstraction layer**: pluggable gRPC and Netty TCP implementations
- **Prometheus metrics**: counters, timers, and gauges for all Raft events
- **Durable storage** behind a `RaftStorage` interface with `BerkeleyDbStorage`
  (synchronous-commit) and `InMemoryStorage` (tests/demos)
- **Spring IoC** wiring with `@ConditionalOnMissingBean` for overridable components
- **Docker** deployment configuration

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
│   ├── architecture.md             # source for the PDF
│   ├── component.puml              # PlantUML: system overview
│   ├── class.puml                  # PlantUML: core class diagram
│   ├── election.puml               # PlantUML: PreVote + election sequence
│   ├── replication.puml            # PlantUML: replication pipeline
│   └── snapshot.puml               # PlantUML: chunked snapshot transfer
└── src/
    ├── main/
    │   ├── proto/
    │   │   ├── raft.proto              # RequestVote, AppendEntries, InstallSnapshot, PreVote
    │   │   └── client.proto            # Submit, AddServer, RemoveServer
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
    │       │   ├── GrpcTransport.java            # gRPC client implementation
    │       │   ├── GrpcTransportFactory.java     # gRPC factory
    │       │   ├── GrpcTransportServer.java      # gRPC server
    │       │   ├── NettyTransport.java           # Netty TCP client
    │       │   ├── NettyTransportFactory.java    # Netty factory
    │       │   ├── NettyTransportServer.java     # Netty TCP server
    │       │   └── NettyProtocol.java            # wire format constants
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

When `metrics.port` is configured in the node's `.properties` file, a
Prometheus-compatible `/metrics` endpoint is exposed:

```
curl http://localhost:10091/metrics
```

See [docs/architecture.pdf](docs/architecture.pdf) for the full metrics table.

## What is *not* implemented

- **Client request de-duplication** (§8): retried commands may be applied twice.
  A production system would track per-client serial numbers.

## License

[Apache License 2.0](LICENSE)
