/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.transport.GrpcTransport;
import com.fbtberger.raft.transport.GrpcTransportServer;
import com.fbtberger.raft.transport.RaftTransport;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration-level tests for a three-node cluster using gRPC's
 * {@link InProcessServerBuilder} / {@link InProcessChannelBuilder}: no
 * network, no Berkeley DB, but real {@link RaftNode} instances talking to
 * each other through real gRPC stubs over an in-memory transport. This
 * gives us full end-to-end coverage of leader election, log replication,
 * majority commit, and snapshot propagation without any external
 * dependencies and without the need to subclass the generated (private-
 * constructor) stub classes.
 */
class ThreeNodeClusterTest {

    // Initial 3-node peer map; address is used as the in-process server name
    // so no address-to-id translation is needed in the stub factory.
    private static final Map<String, String> INITIAL_PEERS = Map.of(
            "n1", "localhost:9091",
            "n2", "localhost:9092",
            "n3", "localhost:9093");

    private Map<String, RaftNode>            nodes;
    private Map<String, KeyValueStateMachine> machines;
    private Map<String, InMemoryStorage>     stores;
    private Map<String, Server>              grpcServers;
    private List<RaftTransport>             transports;

    @BeforeEach
    void setUp() throws Exception {
        nodes      = new HashMap<>();
        machines   = new HashMap<>();
        stores     = new HashMap<>();
        grpcServers = new HashMap<>();
        transports = new ArrayList<>();

        startNodes(INITIAL_PEERS, /*snapshotThreshold=*/ 5);
        for (RaftNode n : nodes.values()) n.start();
        awaitLeader(2_000);
    }

    @AfterEach
    void tearDown() {
        for (RaftNode n   : nodes.values())      n.shutdown();
        for (RaftTransport t : transports)      t.close();
        for (Server s     : grpcServers.values()) s.shutdown();
    }

    // ------------------------------------------------------------------
    // Helpers shared by all tests
    // ------------------------------------------------------------------

    /**
     * Creates RaftNodes and matching in-process gRPC servers for every id
     * in {@code peers} that hasn't been started yet. The stub factory uses
     * the peer's address directly as the in-process server name, so it
     * never needs an address-to-id reverse-lookup and naturally supports
     * nodes added later (like a 4th member in the snapshot test).
     */
    private void startNodes(Map<String, String> peers, int threshold) throws Exception {
        for (Map.Entry<String, String> e : peers.entrySet()) {
            String id      = e.getKey();
            String address = e.getValue();
            if (nodes.containsKey(id)) continue; // already running

            InMemoryStorage      store = new InMemoryStorage();
            KeyValueStateMachine sm    = new KeyValueStateMachine();
            stores.put(id, store);
            machines.put(id, sm);

            RaftConfig cfg = config(id, peers, threshold);
            RaftNode node = new RaftNode(cfg, store, sm, peerAddress -> {
                ManagedChannel ch = InProcessChannelBuilder
                        .forName(peerAddress).directExecutor().build();
                GrpcTransport t = new GrpcTransport(ch);
                transports.add(t);
                return t;
            }, RaftMetrics.noop());
            nodes.put(id, node);

            Server srv = InProcessServerBuilder.forName(address)
                    .addService(new GrpcTransportServer.RaftServiceAdapter(node))
                    .build()
                    .start();
            grpcServers.put(id, srv);
        }
    }

    private RaftNode leader() {
        return nodes.values().stream()
                .filter(n -> n.role() == ServerRole.LEADER)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no leader in cluster"));
    }

    private String leaderId() {
        return nodes.entrySet().stream()
                .filter(e -> e.getValue().role() == ServerRole.LEADER)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow();
    }

    private void awaitLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (nodes.values().stream().anyMatch(n -> n.role() == ServerRole.LEADER)) return;
            Thread.sleep(20);
        }
        fail("No leader elected within " + timeoutMs + " ms");
    }

    private void awaitLeaderReady(long timeoutMs) throws InterruptedException {
        awaitCondition(() -> {
            try {
                leader().submitCommand("NOOP".getBytes()).get(100, java.util.concurrent.TimeUnit.MILLISECONDS);
                return true;
            } catch (Exception e) {
                return false;
            }
        }, timeoutMs);
    }

    private void awaitCondition(BooleanSupplier cond, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            Thread.sleep(20);
        }
        fail("Condition not met within " + timeoutMs + " ms");
    }

    // ------------------------------------------------------------------
    // Election tests
    // ------------------------------------------------------------------

    @Test
    void onlyOneLeaderPerTerm() {
        long leaders = nodes.values().stream()
                .filter(n -> n.role() == ServerRole.LEADER)
                .count();
        assertEquals(1, leaders, "exactly one leader must exist in a stable 3-node cluster");
    }

    @Test
    void allNodesAgreeOnCurrentLeader() throws Exception {
        // Wait until all nodes have received at least one heartbeat from the
        // leader rather than sleeping a fixed amount, which is fragile on
        // slow CI machines. Followers record the leader's id in
        // currentLeaderId upon accepting any AppendEntries RPC.
        String leaderId = leader().currentLeaderId();
        assertNotNull(leaderId);
        awaitCondition(
                () -> nodes.values().stream().allMatch(n -> leaderId.equals(n.currentLeaderId())),
                1_000);
        for (RaftNode n : nodes.values()) {
            assertEquals(leaderId, n.currentLeaderId(),
                    "all nodes must agree on who the leader is after heartbeats");
        }
    }

    // ------------------------------------------------------------------
    // Replication tests (§5.3)
    // ------------------------------------------------------------------

    @Test
    void commandReplicatedToAllFollowers() throws Exception {
        leader().submitCommand("SET shared value".getBytes(StandardCharsets.UTF_8))
                .get(2, TimeUnit.SECONDS);

        // Wait for the follower apply loop to catch up
        awaitCondition(
                () -> machines.values().stream().allMatch(sm -> "value".equals(sm.get("shared"))),
                1_000);

        for (KeyValueStateMachine sm : machines.values()) {
            assertEquals("value", sm.get("shared"),
                    "every node's state machine should reflect the committed value");
        }
    }

    @Test
    void multipleCommandsAppliedInOrderOnAllNodes() throws Exception {
        leader().submitCommand("SET k first".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        leader().submitCommand("SET k second".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);

        awaitCondition(
                () -> machines.values().stream().allMatch(sm -> "second".equals(sm.get("k"))),
                1_000);

        for (KeyValueStateMachine sm : machines.values()) {
            assertEquals("second", sm.get("k"));
        }
    }

    @Test
    void majorityCommitSucceeds() throws Exception {
        // With 3 nodes the majority is 2 (leader + 1). Even if one follower
        // is slow, the commit should complete quickly.
        byte[] result = leader()
                .submitCommand("SET x majority".getBytes(StandardCharsets.UTF_8))
                .get(2, TimeUnit.SECONDS);
        assertEquals("OK", new String(result, StandardCharsets.UTF_8));
    }

    // ------------------------------------------------------------------
    // Log matching property (§5.3)
    // ------------------------------------------------------------------

    @Test
    void followersLogLengthMatchesLeaderAfterReplication() throws Exception {
        leader().submitCommand("SET a 1".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        leader().submitCommand("SET b 2".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);

        String lid = leaderId();
        long expected = stores.get(lid).getLastLogIndex();
        awaitCondition(
                () -> stores.values().stream().allMatch(s -> s.getLastLogIndex() == expected),
                1_000);

        for (Map.Entry<String, InMemoryStorage> e : stores.entrySet()) {
            assertEquals(expected, e.getValue().getLastLogIndex(),
                    e.getKey() + " log length should match the leader's");
        }
    }

    // ------------------------------------------------------------------
    // Cluster reconfiguration (§6)
    // ------------------------------------------------------------------

    @Test
    void addServerExpandsCluster() throws Exception {
        Map<String, String> allPeers = new HashMap<>(INITIAL_PEERS);
        allPeers.put("n4", "localhost:9094");

        // Register n4's in-process gRPC server so the leader can reach it
        // as soon as addServer propagates the config entry. We do NOT call
        // n4.start() yet: until start() is called, n4's resetElectionTimer()
        // is a no-op, so n4 can safely receive RPCs from the leader without
        // scheduling its own election timer and disrupting the cluster with
        // spurious term inflation.
        startNodes(allPeers, 5);
        awaitLeaderReady(2_000);

        leader().addServer("n4", "localhost:9094").get(2, TimeUnit.SECONDS);

        // n4 is now an officially committed cluster member; safe to start.
        nodes.get("n4").start();

        assertTrue(leader().currentConfiguration().containsKey("n4"),
                "n4 should be part of the leader's effective configuration");
    }

    @Test
    void removeServerShrinksCluster() throws Exception {
        awaitLeaderReady(2_000);
        String lid = leaderId();
        String follower = nodes.keySet().stream()
                .filter(id -> !id.equals(lid)).findFirst().orElseThrow();

        leader().removeServer(follower).get(2, TimeUnit.SECONDS);

        assertFalse(leader().currentConfiguration().containsKey(follower),
                follower + " should no longer be in the configuration after removal");
    }

    // ------------------------------------------------------------------
    // Log compaction / snapshotting (§7)
    // ------------------------------------------------------------------

    @Test
    void everyNodeCanTakeIndependentSnapshot() throws Exception {
        leader().submitCommand("SET snap me".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        awaitCondition(
                () -> machines.values().stream().allMatch(sm -> "me".equals(sm.get("snap"))),
                1_000);

        for (Map.Entry<String, RaftNode> e : nodes.entrySet()) {
            e.getValue().snapshotNow();
            assertTrue(e.getValue().snapshotIndex() > 0,
                    e.getKey() + " should have snapshotIndex > 0 after snapshotNow()");
        }
    }

    /**
     * Tests the full InstallSnapshot path end-to-end: the leader compacts
     * its log, then a brand-new node (n4 with empty storage) joins the
     * cluster. Its nextIndex on the leader defaults to 1, which is ≤ the
     * leader's snapshotIndex, so the leader sends InstallSnapshot instead
     * of AppendEntries to catch it up.
     */
    @Test
    void freshFollowerCatchesUpViaInstallSnapshot() throws Exception {
        // Commit entries and make sure they replicate everywhere first
        leader().submitCommand("SET x 10".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        leader().submitCommand("SET y 20".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        awaitCondition(
                () -> machines.values().stream()
                        .allMatch(sm -> "10".equals(sm.get("x")) && "20".equals(sm.get("y"))),
                1_000);

        // Leader compacts everything it has applied so far
        leader().snapshotNow();
        long leaderSnap = leader().snapshotIndex();
        assertTrue(leaderSnap > 0, "leader must have a snapshot after snapshotNow()");

        // Register n4's in-process gRPC server BEFORE addServer, but do NOT
        // call n4.start() yet. Until start() is called, n4's resetElectionTimer()
        // is a no-op (started=false guard added to RaftNode), so n4 can accept
        // the leader's InstallSnapshot RPC without accidentally scheduling an
        // election timer that would inflate terms and disrupt the cluster.
        Map<String, String> allPeers = new HashMap<>(INITIAL_PEERS);
        allPeers.put("n4", "localhost:9094");
        startNodes(allPeers, 5);  // creates RaftNode + in-process server for n4

        // addServer: leader appends a config entry and immediately tries to
        // replicate to n4. n4's nextIndex defaults to 1 ≤ leaderSnap, so
        // replicateTo switches to sendInstallSnapshot. With directExecutor()
        // the round-trip is synchronous, so n4's state machine is restored
        // within the addServer call itself.
        leader().addServer("n4", "localhost:9094").get(2, TimeUnit.SECONDS);

        // n4 is now an officially committed member; safe to start its timer.
        nodes.get("n4").start();

        // Snapshot was installed synchronously above; awaitCondition handles
        // any edge case where the apply loop is still one heartbeat behind.
        awaitCondition(() -> "10".equals(machines.get("n4").get("x")), 500);

        assertEquals("10", machines.get("n4").get("x"),
                "n4 should have received x via InstallSnapshot");
        assertEquals("20", machines.get("n4").get("y"),
                "n4 should have received y via InstallSnapshot");
    }

    // ---- linearizable reads -----------------------------------------------

    @Test
    void readIndexCompletesOnMultiNodeCluster() throws Exception {
        leader().submitCommand("SET rk rv".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        leader().readIndex().get(2, TimeUnit.SECONDS);
    }

    @Test
    void leaseReadCompletesOnMultiNodeCluster() throws Exception {
        leader().submitCommand("SET lk lv".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        // A lease read needs a lease, which needs a heartbeat round the leader has had time to
        // complete. Waiting for the read to succeed says that directly; a hundred milliseconds
        // only said it on this machine.
        Await.until("a lease read completes", 5_000, () -> {
            try { leader().leaseRead().get(2, TimeUnit.SECONDS); return true; }
            catch (Exception e) { return false; }
        });
    }

    // ------------------------------------------------------------------
    // Non-voting learners (§4.2.1)
    // ------------------------------------------------------------------

    @Test
    void learnerReceivesReplicationWithoutAffectingQuorumThenIsPromoted() throws Exception {
        awaitLeaderReady(2_000);

        // Commit a value before the learner exists, so the learner has to
        // catch up on log it never saw live.
        leader().submitCommand("SET x 1".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);

        // Bring up n4 as a non-voting learner: gRPC server running, but not
        // start()ed yet (started=false keeps its election timer disarmed).
        startLearnerNode("n4", "localhost:9094", INITIAL_PEERS, 5);
        leader().addLearner("n4", "localhost:9094").get(2, TimeUnit.SECONDS);
        nodes.get("n4").start();

        // n4 is a learner, not a voting member, on the leader's view.
        assertTrue(leader().currentLearners().containsKey("n4"),
                "n4 should be a learner on the leader");
        assertFalse(leader().currentConfiguration().containsKey("n4"),
                "n4 must NOT be a voting member yet");

        // A fresh command still commits: it only needs the 3 voters, proving
        // the learner does not enlarge the majority. It must also reach the
        // learner's own state machine, catching up the earlier entry too.
        leader().submitCommand("SET y 2".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        awaitCondition(() -> "2".equals(machines.get("n4").get("y"))
                && "1".equals(machines.get("n4").get("x")), 1_000);

        // The learner never runs for or wins leadership.
        assertSame(ServerRole.FOLLOWER, nodes.get("n4").role(), "a learner must never become leader");
        assertTrue(nodes.get("n4").isLearner(), "n4 should report itself as a learner");

        // Once caught up, promotion succeeds and moves n4 into the voting set.
        awaitCondition(() -> {
            try {
                leader().promoteLearner("n4").get(500, TimeUnit.MILLISECONDS);
                return true;
            } catch (Exception e) {
                return false; // still catching up: matchIndex < commitIndex
            }
        }, 2_000);
        assertTrue(leader().currentConfiguration().containsKey("n4"),
                "n4 should be a voting member after promotion");
        assertFalse(leader().currentLearners().containsKey("n4"),
                "n4 should no longer be listed as a learner after promotion");
    }

    @Test
    void promoteRejectsUnknownLearner() throws Exception {
        awaitLeaderReady(2_000);
        ExecutionException ex = assertThrows(ExecutionException.class,
                () -> leader().promoteLearner("does-not-exist").get(2, TimeUnit.SECONDS));
        assertInstanceOf(RaftNode.ConfigurationChangeException.class, ex.getCause());
    }

    /**
     * Creates a RaftNode configured as a non-voting learner (node.learner=true)
     * plus its in-process gRPC server, mirroring {@link #startNodes} but for a
     * single learner. Not start()ed here: the caller starts it only after the
     * leader has admitted it, exactly like the addServer flow.
     */
    private void startLearnerNode(String id, String address, Map<String, String> voters, int threshold)
            throws Exception {
        InMemoryStorage store = new InMemoryStorage();
        KeyValueStateMachine sm = new KeyValueStateMachine();
        stores.put(id, store);
        machines.put(id, sm);

        RaftConfig cfg = learnerConfig(id, address, voters, threshold);
        RaftNode node = new RaftNode(cfg, store, sm, peerAddress -> {
            ManagedChannel ch = InProcessChannelBuilder.forName(peerAddress).directExecutor().build();
            GrpcTransport t = new GrpcTransport(ch);
            transports.add(t);
            return t;
        }, RaftMetrics.noop());
        nodes.put(id, node);

        Server srv = InProcessServerBuilder.forName(address)
                .addService(new GrpcTransportServer.RaftServiceAdapter(node))
                .build()
                .start();
        grpcServers.put(id, srv);
    }

    private static RaftConfig learnerConfig(String id, String address, Map<String, String> voters, int threshold)
            throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", id);
        props.setProperty("node.port", String.valueOf(Integer.parseInt(address.split(":")[1])));
        props.setProperty("data.dir", "/tmp/raft-cluster-test-unused/" + id);
        for (Map.Entry<String, String> e : voters.entrySet()) {
            props.setProperty("peer." + e.getKey(), e.getValue());
        }
        props.setProperty("peer." + id, address);
        props.setProperty("node.learner", "true");
        props.setProperty("snapshot.threshold", String.valueOf(threshold));

        Path tmp = Files.createTempFile("raft-learner-" + id + "-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    // ------------------------------------------------------------------
    // Static helpers
    // ------------------------------------------------------------------

    private static RaftConfig config(String id, Map<String, String> peers, int threshold)
            throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", id);
        // Port is encoded in the address, but RaftConfig.load() needs it separately
        String address = peers.get(id);
        int port = Integer.parseInt(address.split(":")[1]);
        props.setProperty("node.port", String.valueOf(port));
        props.setProperty("data.dir", "/tmp/raft-cluster-test-unused/" + id);
        for (Map.Entry<String, String> e : peers.entrySet()) {
            props.setProperty("peer." + e.getKey(), e.getValue());
        }
        props.setProperty("snapshot.threshold", String.valueOf(threshold));

        Path tmp = Files.createTempFile("raft-cluster-" + id + "-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }
}
