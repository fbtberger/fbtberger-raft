/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.PreVoteRequest;
import com.fbtberger.raft.proto.PreVoteResponse;
import com.fbtberger.raft.proto.ReadIndexRequest;
import com.fbtberger.raft.proto.ReadIndexResponse;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import com.fbtberger.raft.transport.GrpcTransport;
import com.fbtberger.raft.transport.GrpcTransportServer;
import com.fbtberger.raft.transport.RaftTransport;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two data nodes and a witness, over the real gRPC transport in-process.
 *
 * <p>The case the whole design turns on is {@link #theWitnessCarriesAnEntryAcrossThatOnlyItAndTheDeadLeaderHeld}:
 * an entry committed by the leader and the witness, the leader gone, the remaining data node
 * one entry short. A witness that merely voted could not vote for that node and nobody else
 * could lead; the cluster would stand still with two of three nodes up.
 */
class WitnessClusterTest {

    private static final AtomicInteger RUN = new AtomicInteger();

    private final Map<String, String> addresses = new LinkedHashMap<>();
    private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
    private final Map<String, KeyValueStateMachine> machines = new LinkedHashMap<>();
    private final Map<String, KeyValueStateMachine> named = new LinkedHashMap<>();
    private final Map<String, InMemoryStorage> stores = new LinkedHashMap<>();
    private final Map<String, Server> servers = new LinkedHashMap<>();
    private final List<RaftTransport> transports = new ArrayList<>();
    /** "from>to" pairs whose messages are lost. */
    private final Set<String> cut = ConcurrentHashMap.newKeySet();
    /** "from>to" pairs over which an InstallSnapshot was attempted. */
    private final Set<String> snapshotsSent = ConcurrentHashMap.newKeySet();

    @AfterEach
    void tearDown() {
        nodes.values().forEach(RaftNode::shutdown);
        transports.forEach(RaftTransport::close);
        servers.values().forEach(Server::shutdownNow);
    }

    @Test
    void theClusterWorksAndTheWitnessHoldsTheLogAndNoState() throws Exception {
        start(/*witnessRetain*/ 1_000, /*threshold*/ 1_000);
        String leader = awaitDataLeader();

        for (int i = 1; i <= 5; i++) {
            write(leader, "SET k" + i + " " + i);
        }

        Await.until("the witness holds the log", 3_000,
                () -> stores.get("w").getLastLogIndex() == stores.get(leader).getLastLogIndex()
                        && nodes.get("w").appliedIndex() == nodes.get(leader).appliedIndex());
        assertNull(machines.get("w").get("k1"), "the state machine a witness is wired with stays empty");
        assertEquals("5", machines.get(leader).get("k5"));
    }

    @Test
    void aCommandForANamedStateMachineCommitsWithTheWitnessWhichKeepsNoneOfIt() throws Exception {
        start(1_000, 1_000);
        String leader = awaitDataLeader();
        isolate(otherDataNode(leader));

        // Committed by the leader and the witness alone, so the witness has applied an
        // entry for a name its own registry does not hold -- and must not fail on it.
        nodes.get(leader).submitCommand("values", "SET issued 7".getBytes(StandardCharsets.UTF_8))
                .get(3, TimeUnit.SECONDS);
        write(leader, "SET k 1");

        Await.until("the witness applied both", 3_000,
                () -> nodes.get("w").appliedIndex() == nodes.get(leader).appliedIndex());
        assertEquals("7", named.get(leader).get("issued"));
        assertNull(named.get("w").get("issued"));
        assertNull(machines.get("w").get("k"));
        assertTrue(nodes.get("w").stateMachineNames().isEmpty());
    }

    @Test
    void oneDataNodeAndTheWitnessAreAMajority() throws Exception {
        start(1_000, 1_000);
        String leader = awaitDataLeader();
        String other = otherDataNode(leader);
        write(leader, "SET before 1");

        isolate(other);
        write(leader, "SET during 2");

        assertEquals("2", machines.get(leader).get("during"),
                "with a data node down, the witness's acknowledgement is what commits");
    }

    @Test
    void whenTheLeaderFailsTheOtherDataNodeLeadsNotTheWitness() throws Exception {
        start(1_000, 1_000);
        String leader = awaitDataLeader();
        String other = otherDataNode(leader);
        write(leader, "SET a 1");
        Await.until("the other data node is level", 3_000,
                () -> "1".equals(machines.get(other).get("a")));

        isolate(leader);

        Await.until(other + " takes the lead", 5_000, () -> nodes.get(other).role() == ServerRole.LEADER);
        write(other, "SET b 2");
        assertEquals("2", machines.get(other).get("b"));
    }

    @Test
    void theWitnessCarriesAnEntryAcrossThatOnlyItAndTheDeadLeaderHeld() throws Exception {
        start(1_000, 1_000);
        String leader = awaitDataLeader();
        String other = otherDataNode(leader);
        write(leader, "SET seen-by-all 1");
        Await.until("the other data node is level", 3_000,
                () -> "1".equals(machines.get(other).get("seen-by-all")));

        // The other data node stops hearing the leader: what is written now is committed
        // by the leader and the witness alone.
        cut.add(leader + ">" + other);
        write(leader, "SET only-on-witness 2");
        write(leader, "SET and-this 3");
        assertNull(machines.get(other).get("only-on-witness"));

        // The leader dies. The survivor is two committed entries short, and the only
        // other copy of them is in the log of a node with no state machine.
        isolate(leader);

        Await.until(other + " leads with everything that was committed", 10_000,
                () -> nodes.get(other).role() == ServerRole.LEADER
                        && "2".equals(machines.get(other).get("only-on-witness"))
                        && "3".equals(machines.get(other).get("and-this")));
        assertEquals("1", machines.get(other).get("seen-by-all"));
        assertNotEquals(ServerRole.LEADER, nodes.get("w").role(), "the witness led only to hand over");

        write(other, "SET afterwards 4");
        assertEquals("4", machines.get(other).get("afterwards"));
    }

    @Test
    void aLeadingWitnessAnswersNoClient() throws Exception {
        start(1_000, 1_000);
        String leader = awaitDataLeader();
        String other = otherDataNode(leader);
        write(leader, "SET a 1");
        Await.until("level", 3_000, () -> "1".equals(machines.get(other).get("a")));

        // Nobody left for the witness to hand the lead to: it wins and has to keep it.
        cut.add(leader + ">" + other);
        write(leader, "SET b 2");
        isolate(leader);
        cut.add("w>" + other + "#append");
        Await.until("the witness leads", 10_000, () -> nodes.get("w").role() == ServerRole.LEADER);

        ExecutionException refused = assertThrows(ExecutionException.class,
                () -> nodes.get("w").submitCommand("SET c 3".getBytes()).get(2, TimeUnit.SECONDS));
        assertInstanceOf(RaftNode.NotLeaderException.class, refused.getCause());
        ExecutionException readRefused = assertThrows(ExecutionException.class,
                () -> nodes.get("w").query("GET a".getBytes()).get(2, TimeUnit.SECONDS));
        assertInstanceOf(RaftNode.NotLeaderException.class, readRefused.getCause());
    }

    @Test
    void aWitnessNeverInstallsItsEmptySnapshotOnADataNode() throws Exception {
        // Retain nothing, compact eagerly: the witness's log starts after its commit index.
        start(/*witnessRetain*/ 0, /*threshold*/ 2);
        String leader = awaitDataLeader();
        String other = otherDataNode(leader);
        write(leader, "SET precious 1");
        Await.until("level", 3_000, () -> "1".equals(machines.get(other).get("precious")));

        cut.add(leader + ">" + other);
        for (int i = 0; i < 6; i++) {
            write(leader, "SET later" + i + " " + i);
        }
        Await.until("the witness has compacted past what the other data node holds", 3_000,
                () -> nodes.get("w").snapshotIndex() > stores.get(other).getLastLogIndex());
        isolate(leader);
        Await.until("the witness leads", 10_000, () -> nodes.get("w").role() == ServerRole.LEADER);

        // Several heartbeat rounds in which the witness finds the data node behind its log.
        Thread.sleep(600);

        assertTrue(snapshotsSent.stream().noneMatch(pair -> pair.startsWith("w>")),
                "a witness's snapshot is empty; installed, it would erase the data node: " + snapshotsSent);
        assertEquals("1", machines.get(other).get("precious"));
        assertEquals(ServerRole.LEADER, nodes.get("w").role(),
                "it keeps the lead until a data node can take it, and waits for one that can catch the other up");
    }

    @Test
    void transferringTheLeadToAWitnessFailsAtOnceAndSaysWhy() throws Exception {
        start(1_000, 1_000);
        String leader = awaitDataLeader();
        write(leader, "SET a 1");
        Await.until("the witness is level", 3_000,
                () -> stores.get("w").getLastLogIndex() == stores.get(leader).getLastLogIndex());

        long startedAt = System.nanoTime();
        ExecutionException refused = assertThrows(ExecutionException.class,
                () -> nodes.get(leader).transferLeadership("w").get(5, TimeUnit.SECONDS));

        assertTrue(refused.getCause().getMessage().contains("is a witness"), refused.getCause().getMessage());
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt) < 250,
                "answered by the witness, not by the transfer timeout");
        // And the leader is back in service straight away.
        write(leader, "SET b 2");
        assertEquals(ServerRole.LEADER, nodes.get(leader).role());
    }

    // ---- harness -----------------------------------------------------------

    private void start(int witnessRetain, int threshold) throws Exception {
        int run = RUN.incrementAndGet();
        addresses.put("a", "witness-" + run + "-a:9701");
        addresses.put("b", "witness-" + run + "-b:9702");
        addresses.put("w", "witness-" + run + "-w:9703");
        for (String id : addresses.keySet()) {
            RaftConfig config = config(id, threshold);
            if (id.equals("w")) {
                config = config.asWitness(witnessRetain);
            }
            InMemoryStorage store = new InMemoryStorage();
            KeyValueStateMachine machine = new KeyValueStateMachine();
            KeyValueStateMachine namedMachine = new KeyValueStateMachine();
            named.put(id, namedMachine);
            // The witness is wired like every node, named state machine included.
            RaftNode node = new RaftNode(config, store,
                    new StateMachineRegistry(machine, Map.of("values", namedMachine)), address -> {
                GrpcTransport grpc = new GrpcTransport(
                        InProcessChannelBuilder.forName(address).directExecutor().build());
                transports.add(grpc);
                return new CuttableTransport(id, idOf(address), grpc);
            }, RaftMetrics.noop());
            nodes.put(id, node);
            machines.put(id, machine);
            stores.put(id, store);
            servers.put(id, InProcessServerBuilder.forName(addresses.get(id))
                    .addService(new GrpcTransportServer.RaftServiceAdapter(node)).build().start());
        }
        nodes.values().forEach(RaftNode::start);
    }

    private String idOf(String address) {
        return addresses.entrySet().stream().filter(e -> e.getValue().equals(address))
                .map(Map.Entry::getKey).findFirst().orElseThrow();
    }

    private String awaitDataLeader() throws Exception {
        Await.until("a data node leads and has committed in its term", 5_000, () -> {
            for (String id : List.of("a", "b")) {
                if (nodes.get(id).role() == ServerRole.LEADER) {
                    try {
                        nodes.get(id).submitCommand("SET warm up".getBytes()).get(200, TimeUnit.MILLISECONDS);
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                }
            }
            return false;
        });
        return nodes.get("a").role() == ServerRole.LEADER ? "a" : "b";
    }

    private static String otherDataNode(String id) {
        return id.equals("a") ? "b" : "a";
    }

    private void write(String nodeId, String command) throws Exception {
        nodes.get(nodeId).submitCommand(command.getBytes(StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);
    }

    /** Cuts {@code id} off in both directions, as a crash looks from the outside. */
    private void isolate(String id) {
        for (String peer : addresses.keySet()) {
            if (!peer.equals(id)) {
                cut.add(id + ">" + peer);
                cut.add(peer + ">" + id);
            }
        }
    }

    private RaftConfig config(String id, int threshold) throws IOException {
        Properties props = new Properties();
        props.setProperty("node.id", id);
        props.setProperty("node.port", addresses.get(id).split(":")[1]);
        props.setProperty("data.dir", "/tmp/raft-witness-cluster-unused/" + id);
        addresses.forEach((peer, address) -> props.setProperty("peer." + peer, address));
        props.setProperty("snapshot.threshold", Integer.toString(threshold));
        Path file = Files.createTempFile("raft-witness-cluster-", ".properties");
        try (OutputStream out = Files.newOutputStream(file)) {
            props.store(out, null);
        }
        return RaftConfig.load(file);
    }

    /** A transport whose messages can be made to vanish, per direction. */
    private final class CuttableTransport implements RaftTransport {
        private final String pair;
        private final RaftTransport delegate;

        CuttableTransport(String from, String to, RaftTransport delegate) {
            this.pair = from + ">" + to;
            this.delegate = delegate;
        }

        private <T> CompletableFuture<T> lost() {
            return CompletableFuture.failedFuture(new IOException("link " + pair + " is cut"));
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
            return cut.contains(pair) ? lost() : delegate.requestVote(request);
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
            return cut.contains(pair) || cut.contains(pair + "#append") ? lost() : delegate.appendEntries(request);
        }

        @Override
        public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
            snapshotsSent.add(pair);
            return cut.contains(pair) ? lost() : delegate.installSnapshot(request);
        }

        @Override
        public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest request) {
            return cut.contains(pair) ? lost() : delegate.preVote(request);
        }

        @Override
        public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest request) {
            return cut.contains(pair) ? lost() : delegate.timeoutNow(request);
        }

        @Override
        public CompletableFuture<ReadIndexResponse> readIndex(ReadIndexRequest request) {
            return cut.contains(pair) ? lost() : delegate.readIndex(request);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
