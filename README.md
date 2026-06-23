# raft-java

A from-scratch Java implementation of the core Raft consensus algorithm, following
Ongaro & Ousterhout, *"In Search of an Understandable Consensus Algorithm"* (extended
version). RPCs are defined with Protocol Buffers and transported over gRPC; persistent
state (current term, vote, and the log) is stored in Berkeley DB Java Edition with
synchronous (fsync-on-write) durability.

## Project layout

```
raft-java/
├── pom.xml
├── config/
│   ├── node1.properties        # example 3-node demo cluster config
│   ├── node2.properties
│   └── node3.properties
└── src/main/
    ├── proto/
    │   ├── raft.proto          # RequestVote / AppendEntries (Figure 2), ClusterConfiguration (§6), InstallSnapshot (§7) -- internal, server-to-server
    │   └── client.proto        # RaftClientService: Submit, AddServer, RemoveServer (§6) -- external, client-facing
    └── java/com/fbtberger/raft/
        ├── RaftConfig.java         # loads node id/port/peers/data dir/snapshot threshold from .properties
        ├── ServerRole.java         # FOLLOWER / CANDIDATE / LEADER
        ├── RaftStorage.java        # abstract durable-state interface (Figure 2) + snapshot storage (§7)
        ├── BerkeleyDbStorage.java  # RaftStorage impl backed by Berkeley DB JE
        ├── InMemoryStorage.java    # non-durable RaftStorage impl, for tests/demos
        ├── StateMachine.java       # interface applied to committed log entries + snapshot hooks (§7)
        ├── KeyValueStateMachine.java # demo in-memory KV store ("SET key value")
        ├── RaftNode.java           # the algorithm: election, replication, safety
        ├── RaftGrpcService.java    # internal gRPC adapter, delegates to RaftNode
        ├── RaftClientGrpcService.java # client-facing gRPC adapter, delegates to RaftNode
        ├── RaftServer.java         # main(): wiring + interactive demo CLI (server process)
        └── client/
            ├── RaftClient.java         # generic client: leader discovery, retry, transport
            ├── RaftClientException.java
            ├── KeyValueClient.java     # example domain-specific client built on RaftClient
            └── RaftClientDemo.java     # main(): standalone client process (no server code)
```

## Building

```
mvn package
```

This requires internet access — Maven needs to download the gRPC/Protobuf
dependencies and the `protoc`/`protoc-gen-grpc-java` plugin binaries the first time
it runs. **This build has not been compiled or run in the sandbox used to write it**,
since that environment has no network access; please treat it as carefully
hand-written but unverified, and report back if `mvn package` surfaces any issues —
they're likely small (an import, a generated-class name) rather than structural.

A successful build produces a shaded, runnable JAR at
`target/raft-java-1.0-SNAPSHOT.jar` (entry point `com.fbtberger.raft.RaftServer`).

## Running a demo cluster

Open three terminals, one per node:

```
java -jar target/raft-java-1.0-SNAPSHOT.jar config/node1.properties
java -jar target/raft-java-1.0-SNAPSHOT.jar config/node2.properties
java -jar target/raft-java-1.0-SNAPSHOT.jar config/node3.properties
```

Each process prints its own log lines prefixed with its node id, so you can watch
the election happen — after the randomized timeout (150–300 ms) one node will become
CANDIDATE, request votes, and become LEADER once it has a majority.

Each node also runs an interactive CLI on stdin:

```
SET foo bar      # submits a command through Raft, waits for it to commit
GET foo          # reads directly from this node's local state machine
STATUS           # prints this node's current role, known leader, configuration, and snapshot boundary
ADD node4 host:port  # adds a new voting member to the cluster (§6) -- leader only
REMOVE node4         # removes an existing voting member from the cluster (§6) -- leader only
SNAPSHOT         # forces an immediate log-compaction snapshot (§7), bypassing the configured threshold
quit             # shuts the node down
```

Example session against the leader:

```
> SET foo bar
OK
> GET foo
bar
> STATUS
role=LEADER leader=node1
```

If you run `SET` against a follower, it replies with `not leader; try <leader-id>`
instead of forwarding the request — this is a learning/demo implementation, not a
production client library.

## Client layer

Talking to the cluster doesn't require embedding a `RaftNode` — `client.proto`
defines a small, separate `RaftClientService.Submit` RPC just for this, and
`com.fbtberger.raft.client.RaftClient` is a generic client built on it: given a map
of node id → address, it submits a command to its best guess at the leader, follows
the `leader_hint` if it guessed wrong, and falls back to trying every other node if
it doesn't have a hint. It knows nothing about what a command means — just bytes in,
bytes out — so it's reusable across different applications and command encodings.

`KeyValueClient` is an example of a thin, domain-specific client built on top: it
just encodes/decodes this repo's demo "SET key value" text commands and leaves
everything else (leader discovery, retries, transport) to `RaftClient`. A different
state machine with a different command format would write an equivalent wrapper of
its own.

`RaftClientDemo` runs this as a real standalone process, separate from any server:

```
java -cp target/raft-java-1.0-SNAPSHOT.jar com.fbtberger.raft.client.RaftClientDemo config/node1.properties
```

Any one of the three `config/node*.properties` files works here — the demo only
reads the shared `peer.*` address book out of it, ignoring the fields that are only
meaningful to a server process (`node.id`, `node.port`, `data.dir`). Once connected:

```
Connected to cluster: [node1, node2, node3]
Commands: SET <key> <value> | ADD <id> <host:port> | REMOVE <id> | quit
> SET foo bar
OK
```

Note that `-cp` (not `-jar`) is used here: the shaded jar's manifest points at
`RaftServer` as its main class, but since it bundles every dependency, any class
inside it — including `RaftClientDemo` — can be run directly off the classpath.

## Cluster reconfiguration (§6)

The cluster's membership can change while it's running, via `ADD`/`REMOVE` on
either CLI above, or `RaftClient.addServer` / `RaftClient.removeServer` if you're
driving it from code. Internally these go through `RaftNode.addServer` /
`RaftNode.removeServer`, which only the current leader will accept.

The paper offers two ways to do this safely: full joint consensus (a two-phase
`C_old,new` configuration that's harder to implement) or the simpler alternative
it also describes — changing **one server at a time**. This implementation uses
the latter: because any majority of the old configuration and any majority of a
configuration that differs by one member always overlap by at least one server,
moving through configurations one server at a time can never let an old and a new
leader be elected for the same term, without needing joint consensus's extra
machinery. A configuration change is just a new kind of log entry (alongside the
regular command entries), replicated, committed, and applied exactly like any
other entry, with two extra rules from §6: every server uses the latest
configuration in its own log for vote-counting and commit decisions even before
that entry is committed, and a leader that commits a configuration which drops
itself immediately steps down. Only one configuration change is allowed in
flight at a time — a new `ADD`/`REMOVE` is rejected while the most recent one is
still uncommitted.

Two related things from the paper are **not** implemented, and are worth knowing
about before relying on this for anything beyond learning/demos:

- The disruption-prevention refinement (a server ignores `RequestVote` if it has
  heard from a leader recently) isn't implemented. A server that was just
  `REMOVE`d but is still running could in theory still send `RequestVote`s at an
  incremented term and disrupt an election, even though it's no longer a member.
- The generic `RaftClient`'s address book is fixed at construction from the
  `peerAddresses` it was given; a bare `leader_hint` is just a node id, not an
  address, so a node `ADD`ed after a particular client was built isn't directly
  reachable from that client until its address book is updated too (see
  `RaftClient`'s class doc).

## Log compaction / snapshotting (§7)

Left alone, every server's log grows forever, which eventually becomes a problem
for both disk usage and how long a restarting (or far-behind) server takes to
catch up by replaying everything from index 1. §7 addresses this by letting a
server fold its log's prefix into a single **snapshot** of the state machine at
some point, then discard every log entry that snapshot already covers.

Every server here does this **independently**, not just the leader — each one
checks, right after applying newly committed entries, whether enough new
entries have piled up since its own last snapshot (`snapshot.threshold` in its
`.properties` file, default 100; the demo configs under `config/` set it to 20
so you can see compaction happen without sending hundreds of `SET`s first). When
the threshold is hit, it asks the `StateMachine` for a complete snapshot of
itself (`takeSnapshot()`), bundles that with the Raft-level boundary
(`lastIncludedIndex`/`lastIncludedTerm`) and the current §6 cluster
configuration, persists all of it, and discards the log entries that boundary
covers — `RaftStorage.saveSnapshotAndCompact` does the persist-and-discard step
atomically, so a crash mid-compaction can never leave the two halves
disagreeing. You can also trigger this on demand with the `SNAPSHOT` CLI
command, regardless of the threshold.

The reason the cluster configuration is bundled into the snapshot itself,
rather than left purely to the state machine, is the §6+§7 interaction: a
configuration change is just a log entry like any other, so if it's old enough
it can get compacted away by an ordinary snapshot. Without capturing it
separately, a server that restarted (or a follower that caught up purely via a
received snapshot) would have no way to recover what its own membership was.
`RaftNode.recomputeEffectiveConfiguration` accounts for this: it scans the log
backward for a configuration entry only down to the snapshot boundary, and
falls back to the configuration recorded inside the snapshot if it doesn't find
one there.

When the leader notices a follower needs entries it has already compacted away
— nextIndex for that follower has fallen at or below the leader's own snapshot
boundary, which is exactly what happens for a follower that's fallen far behind,
or a brand-new member just `ADD`ed with an empty log — it sends that follower
its snapshot via the **InstallSnapshot** RPC instead of `AppendEntries`. The
follower installs it (discarding its own log prefix, restoring its state
machine, and recomputing its effective configuration the same way), and normal
`AppendEntries` replication picks back up for whatever comes after.

Two simplifications from the paper are intentionally accepted here:

- **No chunking.** The paper's `InstallSnapshot` splits a snapshot across
  multiple RPCs with `offset`/`done` fields, so a leader never has to hold an
  entire (potentially huge) snapshot in memory or in one message at once. This
  implementation always sends the whole snapshot in a single RPC instead —
  simpler to implement and reason about, and fine for this demo-scale key-value
  store, but it means a snapshot's size is bounded by gRPC's max message size
  (a few MB by default) rather than being unbounded the way the paper's version
  is. A deployment with a genuinely large state machine would need to add
  chunking back in.
- **Synchronous, lock-holding compaction.** Taking a snapshot here runs while
  holding the same lock used for all of Raft's other decision-making
  (elections, replication), so a slow `takeSnapshot()` or
  `saveSnapshotAndCompact()` briefly pauses everything else on that server.
  The paper notes production systems avoid this by using copy-on-write or
  forking a child process to write the snapshot in the background; that's out
  of scope here.

## What is implemented

- Leader election with randomized timeouts and the election-restriction safety rule
  (§5.2, §5.4.1): a candidate only gets your vote if its log is at least as
  up-to-date as yours.
- Log replication via `AppendEntries`, including the consistency check, conflicting
  entry truncation, and the "only commit entries from the current term" safety rule
  (§5.3, §5.4.2).
- A no-op entry committed by every new leader at the start of its term, used to
  establish a commit point without relying on stale state (§8).
- A basic client-submission path, reachable two ways: in-process via
  `RaftNode.submitCommand` (used by each server's own CLI), or over the network via
  the separate `RaftClientService` gRPC contract and the generic `RaftClient` /
  `KeyValueClient` client layer described below. Both block until the entry is
  committed and applied, or report who the leader is if you ask the wrong node.
- Durable storage of `currentTerm`, `votedFor`, and the log behind a `RaftStorage`
  interface, with `BerkeleyDbStorage` as the real (synchronous-commit) implementation
  a node restarts correctly from. `RaftNode` only ever talks to the interface, so a
  different storage engine — or `InMemoryStorage`, included for tests and quick
  demos — can be swapped in without touching the algorithm. `InMemoryStorage` is
  **not durable** and must not be used for an actual cluster; see its Javadoc.
- Cluster membership changes (§6), one server at a time, with the dynamic majority
  and leader step-down rules the paper requires for this to stay safe — see the
  dedicated section above for the design and its known limitations.
- Log compaction / snapshotting (§7), taken independently by every server (not
  just the leader) once enough new entries pile up, plus the `InstallSnapshot`
  RPC for catching up a follower that needs entries already compacted away —
  see the dedicated section above for the design and its accepted simplifications.

## What is *not* implemented (out of scope)

- **Client request de-duplication via serial numbers** (§8) — if a client retries a
  `SET` after a timeout (whether via the in-process CLI or the `RaftClient` layer),
  it may be applied twice. A real client library would track per-client serial
  numbers so the state machine can recognize and skip duplicates.

These are exactly the pieces of the paper this implementation deliberately leaves
out; everything in Figure 2 (the RPCs and rules that define core Raft) is implemented.
