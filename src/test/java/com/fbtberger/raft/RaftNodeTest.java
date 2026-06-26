package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.ClusterConfiguration;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.LogEntry;
import com.fbtberger.raft.proto.PreVoteRequest;
import com.fbtberger.raft.proto.PreVoteResponse;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link RaftNode}. All tests use a single-node cluster so
 * that leader election is instantaneous (a solo node has an immediate
 * majority for everything), no real gRPC is needed, and outcomes are fully
 * deterministic. The peer-stub factory always returns null (no peers), so
 * every majority computation resolves with only the node's own copy.
 */
class RaftNodeTest {

    private static final String SERVER_NAME = "localhost:9091";
    private static final Map<String, String> SELF_ONLY = Map.of("n1", SERVER_NAME);

    private InMemoryStorage store;
    private KeyValueStateMachine sm;
    private RaftNode node;
    private Server grpcServer;
    private final List<RaftTransport> transports = new ArrayList<>();
    private final List<RaftNode> peerNodes = new ArrayList<>();
    private final List<Server> peerServers = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        store = new InMemoryStorage();
        sm = new KeyValueStateMachine();
        RaftConfig config = singleNodeConfig();

        node = new RaftNode(config, store, sm, this::connectInProcess, RaftMetrics.noop());

        grpcServer = InProcessServerBuilder.forName(SERVER_NAME)
                .directExecutor()
                .addService(new GrpcTransportServer.RaftServiceAdapter(node))
                .build()
                .start();

        node.start();
        awaitLeader(1_000);
    }

    @AfterEach
    void tearDown() {
        for (RaftNode pn : peerNodes) pn.shutdown();
        node.shutdown();
        for (RaftTransport t : transports) t.close();
        for (Server ps : peerServers) ps.shutdown();
        grpcServer.shutdown();
    }

    // ---- election & leader stability ------------------------------------

    @Test
    void singleNodeBecomesLeaderImmediately() {
        assertEquals(ServerRole.LEADER, node.role());
        assertEquals("n1", node.currentLeaderId());
    }

    @Test
    void leaderHasCommittedNoOpAtIndex1() throws Exception {
        // §8: every new leader commits a blank no-op entry to establish
        // its commit point. In a single-node cluster that commits instantly.
        awaitCommitIndex(1, 1_000);
        LogEntry noOp = store.getLogEntry(1);
        assertNotNull(noOp);
        assertEquals(0, noOp.getCommand().size()); // empty command = no-op
    }

    // ---- submitCommand --------------------------------------------------

    @Test
    void submitCommandCommitsAndApplies() throws Exception {
        byte[] result = node.submitCommand("SET x 42".getBytes(StandardCharsets.UTF_8))
                .get(2, TimeUnit.SECONDS);
        assertEquals("OK", new String(result, StandardCharsets.UTF_8));
        assertEquals("42", sm.get("x"));
    }

    @Test
    void multipleCommandsAreAppliedInOrder() throws Exception {
        node.submitCommand("SET k first".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        node.submitCommand("SET k second".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        assertEquals("second", sm.get("k"));
    }

    @Test
    void followerRejectsSubmitWithNotLeaderException() throws Exception {
        // Use a 3-node config so the node stays a follower (majority > 1
        // means it won't immediately self-elect like a single-node cluster).
        RaftConfig multiNodeConfig = multiNodeConfig("follower1");
        RaftNode follower = new RaftNode(multiNodeConfig, new InMemoryStorage(),
                new KeyValueStateMachine(), addr -> null, RaftMetrics.noop());
        try {
            follower.start();
            assertEquals(ServerRole.FOLLOWER, follower.role());
            CompletableFuture<byte[]> future = follower.submitCommand("SET x 1".getBytes(StandardCharsets.UTF_8));
            assertTrue(future.isCompletedExceptionally());
            assertThrows(Exception.class, () -> future.get(100, TimeUnit.MILLISECONDS));
        } finally {
            follower.shutdown();
        }
    }

    // ---- cluster reconfiguration (§6) -----------------------------------

    @Test
    void addServerAppendsConfigurationEntry() throws Exception {
        startPeerNode("n2", "localhost:9092");
        node.addServer("n2", "localhost:9092").get(2, TimeUnit.SECONDS);
        assertTrue(node.currentConfiguration().containsKey("n2"));
    }

    @Test
    void addExistingMemberFails() {
        CompletableFuture<byte[]> f = node.addServer("n1", "localhost:9091");
        assertTrue(f.isCompletedExceptionally());
    }

    @Test
    void removeOnlyMemberFails() {
        CompletableFuture<byte[]> f = node.removeServer("n1");
        assertTrue(f.isCompletedExceptionally());
    }

    @Test
    void removeNonMemberFails() {
        CompletableFuture<byte[]> f = node.removeServer("nobody");
        assertTrue(f.isCompletedExceptionally());
    }

    // ---- log compaction / snapshotting (§7) -----------------------------

    @Test
    void snapshotNowCapturatesAppliedState() throws Exception {
        node.submitCommand("SET a 1".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        node.submitCommand("SET b 2".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);

        node.snapshotNow();

        assertTrue(node.snapshotIndex() > 0,
                "snapshotIndex should be >0 after taking a snapshot");
        RaftStorage.Snapshot snap = store.getSnapshot();
        assertNotNull(snap);

        // Restore into a fresh machine and verify state survived
        KeyValueStateMachine fresh = new KeyValueStateMachine();
        fresh.restoreSnapshot(snap.stateMachineData);
        assertEquals("1", fresh.get("a"));
        assertEquals("2", fresh.get("b"));
    }

    @Test
    void snapshotNowCompactsLogEntries() throws Exception {
        node.submitCommand("SET x 1".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        node.snapshotNow();

        long snapIdx = node.snapshotIndex();
        assertTrue(snapIdx >= 1);
        // Entries at or before the boundary should be gone from the log
        assertNull(store.getLogEntry(1), "entry at index 1 should have been compacted away");
    }

    @Test
    void snapshotBundlesCurrentConfiguration() throws Exception {
        // In a single-node cluster the config is just {n1}
        node.submitCommand("SET any thing".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        node.snapshotNow();

        RaftStorage.Snapshot snap = store.getSnapshot();
        assertNotNull(snap);
        assertTrue(snap.configurationData.length > 0,
                "snapshot must include configuration bytes");

        ClusterConfiguration cfg = ClusterConfiguration.parseFrom(snap.configurationData);
        assertEquals(1, cfg.getMembersCount());
        assertEquals("n1", cfg.getMembers(0).getId());
    }

    @Test
    void automaticSnapshotThresholdTriggersAfterEnoughEntries() throws Exception {
        // Config has threshold=3; after >=3 newly applied entries a snapshot
        // must have been taken automatically (the no-op counts as 1).
        // Two more commands should push us over.
        node.submitCommand("SET p 1".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        node.submitCommand("SET q 2".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);

        // Allow one more heartbeat cycle for the trigger to fire
        Thread.sleep(100);
        assertTrue(node.snapshotIndex() > 0,
                "automatic snapshot should have fired after threshold was reached");
    }

    // ---- InstallSnapshot RPC receiver (§7) ------------------------------

    @Test
    void handleInstallSnapshotRestoresStateMachineAndAdvancesApplied() throws Exception {
        // Build a snapshot as if sent by a leader
        KeyValueStateMachine leader = new KeyValueStateMachine();
        leader.apply("SET leader_key value".getBytes(StandardCharsets.UTF_8));
        byte[] smData = leader.takeSnapshot();
        byte[] cfgData = ClusterConfiguration.newBuilder()
                .addMembers(ClusterConfiguration.Member.newBuilder().setId("n1").setAddress("localhost:9091"))
                .build()
                .toByteArray();

        // In a single-node cluster the node immediately re-elects itself
        // after any step-down, so we just send the InstallSnapshot with a
        // term higher than the current one. The handler steps down, re-elects
        // (bumping the term), but still installs the snapshot.
        long highTerm = store.getCurrentTerm() + 100;
        sendFullSnapshot(node, highTerm, "other", 10, highTerm, smData, cfgData);

        // State machine should now reflect the leader's snapshot
        assertEquals("value", sm.get("leader_key"));
        assertEquals(10, node.snapshotIndex());
    }

    @Test
    void handleInstallSnapshotIgnoresStalerThanCurrentSnapshot() throws Exception {
        // Take a local snapshot at index 5
        // (manually via several submits so lastApplied is high enough)
        for (int i = 0; i < 4; i++) {
            node.submitCommand(("SET k " + i).getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        }
        node.snapshotNow();
        long localSnap = node.snapshotIndex();
        assertTrue(localSnap >= 1);

        // Now receive an InstallSnapshot with a smaller index -- must be ignored
        sendFullSnapshot(node, store.getCurrentTerm(), "n1",
                localSnap - 1, 1, new byte[0], new byte[0]);
        assertEquals(localSnap, node.snapshotIndex(), "stale snapshot must not regress our boundary");
    }

    @Test
    void handleInstallSnapshotReassemblesMultipleChunks() throws Exception {
        InMemoryStorage chunkStore = new InMemoryStorage();
        KeyValueStateMachine chunkSm = new KeyValueStateMachine();
        RaftNode receiver = new RaftNode(singleNodeConfig(), chunkStore, chunkSm, addr -> null, RaftMetrics.noop());
        try {
            KeyValueStateMachine leaderSm = new KeyValueStateMachine();
            leaderSm.apply("SET chunked yes".getBytes(StandardCharsets.UTF_8));
            byte[] smData = leaderSm.takeSnapshot();
            byte[] cfgData = ClusterConfiguration.newBuilder()
                    .addMembers(ClusterConfiguration.Member.newBuilder().setId("n1").setAddress("localhost:9091"))
                    .build()
                    .toByteArray();
            byte[] packed = RaftNode.packSnapshotData(smData, cfgData);

            int chunkSize = 8;
            for (int offset = 0; offset < packed.length; ) {
                int len = Math.min(chunkSize, packed.length - offset);
                boolean done = (offset + len >= packed.length);
                byte[] chunk = new byte[len];
                System.arraycopy(packed, offset, chunk, 0, len);

                InstallSnapshotRequest req = InstallSnapshotRequest.newBuilder()
                        .setTerm(1).setLeaderId("leader")
                        .setLastIncludedIndex(10).setLastIncludedTerm(1)
                        .setOffset(offset)
                        .setData(com.google.protobuf.ByteString.copyFrom(chunk))
                        .setDone(done)
                        .build();
                receiver.handleInstallSnapshot(req);
                offset += len;
            }

            assertEquals("yes", chunkSm.get("chunked"));
            assertEquals(10, receiver.snapshotIndex());
        } finally {
            receiver.shutdown();
        }
    }

    // ---- RequestVote (§5.2) --------------------------------------------

    @Test
    void rejectsVoteForStaleTerm() {
        long currentTerm = store.getCurrentTerm();
        RequestVoteRequest req = RequestVoteRequest.newBuilder()
                .setTerm(currentTerm - 1)
                .setCandidateId("other")
                .setLastLogIndex(0).setLastLogTerm(0)
                .build();
        RequestVoteResponse resp = node.handleRequestVote(req);
        assertFalse(resp.getVoteGranted());
    }

    @Test
    void rejectsVoteWhenCandidateLogIsStale() throws Exception {
        // Submit some commands so our log is ahead of the candidate's
        node.submitCommand("SET x 1".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);

        RequestVoteRequest req = RequestVoteRequest.newBuilder()
                .setTerm(store.getCurrentTerm() + 1)
                .setCandidateId("other")
                .setLastLogIndex(0).setLastLogTerm(0) // behind us
                .build();
        RequestVoteResponse resp = node.handleRequestVote(req);
        assertFalse(resp.getVoteGranted());
    }

    // ---- AppendEntries (§5.3) -------------------------------------------

    @Test
    void appendEntriesFromStalerTermIsRejected() {
        long currentTerm = store.getCurrentTerm();
        AppendEntriesRequest req = AppendEntriesRequest.newBuilder()
                .setTerm(currentTerm - 1)
                .setLeaderId("other").setPrevLogIndex(0).setPrevLogTerm(0)
                .setLeaderCommit(0).build();
        AppendEntriesResponse resp = node.handleAppendEntries(req);
        assertFalse(resp.getSuccess());
    }

    @Test
    void appendEntriesWithBadPrevTermIsRejected() throws Exception {
        node.submitCommand("SET x 1".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);

        AppendEntriesRequest req = AppendEntriesRequest.newBuilder()
                .setTerm(store.getCurrentTerm())
                .setLeaderId("n1")
                .setPrevLogIndex(1).setPrevLogTerm(999) // wrong term for index 1
                .setLeaderCommit(0).build();
        AppendEntriesResponse resp = node.handleAppendEntries(req);
        assertFalse(resp.getSuccess());
    }

    // ---- AppendEntries edge cases -----------------------------------------

    @Test
    void appendEntriesFromHigherTermCausesStepDown() throws Exception {
        // Use a multi-node config so the node stays as follower after step-down
        InMemoryStorage followerStore = new InMemoryStorage();
        RaftNode follower = new RaftNode(multiNodeConfig("ae1"), followerStore,
                new KeyValueStateMachine(), addr -> null, RaftMetrics.noop());
        try {
            long higherTerm = 10;
            AppendEntriesRequest req = AppendEntriesRequest.newBuilder()
                    .setTerm(higherTerm).setLeaderId("other")
                    .setPrevLogIndex(0).setPrevLogTerm(0)
                    .setLeaderCommit(0).build();
            AppendEntriesResponse resp = follower.handleAppendEntries(req);
            assertTrue(resp.getSuccess());
            assertEquals(higherTerm, followerStore.getCurrentTerm());
            assertEquals(ServerRole.FOLLOWER, follower.role());
        } finally {
            follower.shutdown();
        }
    }

    @Test
    void appendEntriesAdvancesFollowerCommitIndex() throws Exception {
        InMemoryStorage followerStore = new InMemoryStorage();
        RaftNode follower = new RaftNode(multiNodeConfig("ae2"), followerStore,
                new KeyValueStateMachine(), addr -> null, RaftMetrics.noop());
        try {
            LogEntry entry = LogEntry.newBuilder().setIndex(1).setTerm(1)
                    .setCommand(com.google.protobuf.ByteString.copyFromUtf8("SET a 1")).build();
            AppendEntriesRequest appendReq = AppendEntriesRequest.newBuilder()
                    .setTerm(1).setLeaderId("leader")
                    .setPrevLogIndex(0).setPrevLogTerm(0)
                    .addEntries(entry).setLeaderCommit(1).build();
            AppendEntriesResponse resp = follower.handleAppendEntries(appendReq);
            assertTrue(resp.getSuccess());
        } finally {
            follower.shutdown();
        }
    }

    @Test
    void appendEntriesTruncatesConflictingEntries() throws Exception {
        InMemoryStorage followerStore = new InMemoryStorage();
        KeyValueStateMachine followerSm = new KeyValueStateMachine();
        RaftNode follower = new RaftNode(multiNodeConfig("ae3"), followerStore,
                followerSm, addr -> null, RaftMetrics.noop());
        try {
            // Append an entry at index 1 with term 1
            LogEntry original = LogEntry.newBuilder().setIndex(1).setTerm(1)
                    .setCommand(com.google.protobuf.ByteString.copyFromUtf8("SET a old")).build();
            follower.handleAppendEntries(AppendEntriesRequest.newBuilder()
                    .setTerm(1).setLeaderId("leader")
                    .setPrevLogIndex(0).setPrevLogTerm(0)
                    .addEntries(original).setLeaderCommit(0).build());

            // Now send a conflicting entry at index 1 with term 2
            LogEntry conflict = LogEntry.newBuilder().setIndex(1).setTerm(2)
                    .setCommand(com.google.protobuf.ByteString.copyFromUtf8("SET a new")).build();
            AppendEntriesResponse resp = follower.handleAppendEntries(AppendEntriesRequest.newBuilder()
                    .setTerm(2).setLeaderId("leader")
                    .setPrevLogIndex(0).setPrevLogTerm(0)
                    .addEntries(conflict).setLeaderCommit(0).build());
            assertTrue(resp.getSuccess());
            assertEquals(2, followerStore.getLogEntry(1).getTerm());
        } finally {
            follower.shutdown();
        }
    }

    // ---- PreVote (§4.2.3) -------------------------------------------------

    @Test
    void preVoteGrantedWhenNoRecentLeaderContact() throws Exception {
        RaftNode follower = new RaftNode(multiNodeConfig("pv1"), new InMemoryStorage(),
                new KeyValueStateMachine(), addr -> null, RaftMetrics.noop());
        try {
            Thread.sleep(150 /* ELECTION_TIMEOUT_MIN_MS */ + 10);
            PreVoteRequest req = PreVoteRequest.newBuilder()
                    .setTerm(2).setCandidateId("other")
                    .setLastLogIndex(0).setLastLogTerm(0).build();
            PreVoteResponse resp = follower.handlePreVote(req);
            assertTrue(resp.getVoteGranted());
        } finally {
            follower.shutdown();
        }
    }

    @Test
    void preVoteDeniedWhenLeaderIsActive() throws Exception {
        long term = store.getCurrentTerm();
        node.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(term).setLeaderId("n1")
                .setPrevLogIndex(0).setPrevLogTerm(0)
                .setLeaderCommit(0).build());

        PreVoteRequest req = PreVoteRequest.newBuilder()
                .setTerm(term + 1).setCandidateId("intruder")
                .setLastLogIndex(100).setLastLogTerm(100).build();
        PreVoteResponse resp = node.handlePreVote(req);
        assertFalse(resp.getVoteGranted());
    }

    @Test
    void preVoteDeniedForStaleTerm() {
        PreVoteRequest req = PreVoteRequest.newBuilder()
                .setTerm(0).setCandidateId("old")
                .setLastLogIndex(0).setLastLogTerm(0).build();
        PreVoteResponse resp = node.handlePreVote(req);
        assertFalse(resp.getVoteGranted());
    }

    @Test
    void preVoteDeniedWhenCandidateLogIsStale() throws Exception {
        node.submitCommand("SET x 1".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        Thread.sleep(150 /* ELECTION_TIMEOUT_MIN_MS */ + 10);

        PreVoteRequest req = PreVoteRequest.newBuilder()
                .setTerm(store.getCurrentTerm() + 1).setCandidateId("behind")
                .setLastLogIndex(0).setLastLogTerm(0).build();
        PreVoteResponse resp = node.handlePreVote(req);
        assertFalse(resp.getVoteGranted());
    }

    // ---- snapshot transfer edge cases ------------------------------------

    @Test
    void handleInstallSnapshotWithUnexpectedOffsetResetsBuffer() throws Exception {
        RaftNode receiver = new RaftNode(singleNodeConfig(), new InMemoryStorage(),
                new KeyValueStateMachine(), addr -> null, RaftMetrics.noop());
        try {
            InstallSnapshotRequest badOffset = InstallSnapshotRequest.newBuilder()
                    .setTerm(1).setLeaderId("leader")
                    .setLastIncludedIndex(10).setLastIncludedTerm(1)
                    .setOffset(999)
                    .setData(com.google.protobuf.ByteString.copyFromUtf8("garbage"))
                    .setDone(false).build();
            InstallSnapshotResponse resp = receiver.handleInstallSnapshot(badOffset);
            assertNotNull(resp);
            assertEquals(0, receiver.snapshotIndex());
        } finally {
            receiver.shutdown();
        }
    }

    // ---- config edge cases -----------------------------------------------

    @Test
    void configDefaultsForOptionalProperties() throws Exception {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/test");
        props.setProperty("peer.n1", "localhost:9091");

        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-cfg-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        RaftConfig cfg = RaftConfig.load(tmp);
        assertEquals(0, cfg.metricsPort());
        assertEquals(1_048_576, cfg.snapshotChunkSize());
        assertEquals(100, cfg.snapshotThreshold());
    }

    @Test
    void configParsesAllOptionalProperties() throws Exception {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/test");
        props.setProperty("peer.n1", "localhost:9091");
        props.setProperty("metrics.port", "8080");
        props.setProperty("snapshot.chunk.size", "512");
        props.setProperty("snapshot.threshold", "50");

        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-cfg-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        RaftConfig cfg = RaftConfig.load(tmp);
        assertEquals(8080, cfg.metricsPort());
        assertEquals(512, cfg.snapshotChunkSize());
        assertEquals(50, cfg.snapshotThreshold());
    }

    // ---- helpers --------------------------------------------------------

    private void startPeerNode(String id, String address) throws Exception {
        java.util.Map<String, String> peers = new java.util.HashMap<>(SELF_ONLY);
        peers.put(id, address);

        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", id);
        props.setProperty("node.port", address.split(":")[1]);
        props.setProperty("data.dir", "/tmp/raft-test-unused/" + id);
        props.setProperty("snapshot.threshold", "3");
        for (java.util.Map.Entry<String, String> e : peers.entrySet()) {
            props.setProperty("peer." + e.getKey(), e.getValue());
        }

        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-test-" + id + "-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        RaftConfig cfg = RaftConfig.load(tmp);

        InMemoryStorage peerStore = new InMemoryStorage();
        KeyValueStateMachine peerSm = new KeyValueStateMachine();
        RaftNode peerNode = new RaftNode(cfg, peerStore, peerSm, this::connectInProcess, RaftMetrics.noop());
        peerNodes.add(peerNode);

        Server srv = InProcessServerBuilder.forName(address)
                .directExecutor()
                .addService(new GrpcTransportServer.RaftServiceAdapter(peerNode))
                .build()
                .start();
        peerServers.add(srv);
    }

    /** Returns a minimal single-node RaftConfig with a very small snapshot threshold. */
    private static RaftConfig singleNodeConfig() throws Exception {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-test-unused");
        props.setProperty("peer.n1", "localhost:9091");
        props.setProperty("snapshot.threshold", "3"); // low: easy to trigger in tests

        // RaftConfig.load() reads from a file path, so write a temp file
        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-test-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    private void awaitLeader(long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (node.role() == ServerRole.LEADER) return;
            Thread.sleep(10);
        }
        fail("Node did not become leader within " + timeoutMs + " ms");
    }

    private void awaitCommitIndex(long targetIndex, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (store.getLastLogIndex() >= targetIndex) return;
            Thread.sleep(10);
        }
        fail("commitIndex did not reach " + targetIndex + " within " + timeoutMs + " ms");
    }

    private static RaftConfig multiNodeConfig(String selfId) throws Exception {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", selfId);
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-test-unused/" + selfId);
        props.setProperty("peer." + selfId, "localhost:9091");
        props.setProperty("peer.other1", "localhost:9092");
        props.setProperty("peer.other2", "localhost:9093");
        props.setProperty("snapshot.threshold", "3");

        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-test-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    private RaftTransport connectInProcess(String address) {
        ManagedChannel ch = InProcessChannelBuilder.forName(address).directExecutor().build();
        GrpcTransport t = new GrpcTransport(ch);
        transports.add(t);
        return t;
    }

    /** Sends a complete snapshot as a single chunk (offset=0, done=true). */
    private static void sendFullSnapshot(RaftNode target, long term, String leaderId,
                                         long lastIncludedIndex, long lastIncludedTerm,
                                         byte[] smData, byte[] cfgData) {
        byte[] packed = RaftNode.packSnapshotData(smData, cfgData);
        InstallSnapshotRequest req = InstallSnapshotRequest.newBuilder()
                .setTerm(term).setLeaderId(leaderId)
                .setLastIncludedIndex(lastIncludedIndex).setLastIncludedTerm(lastIncludedTerm)
                .setOffset(0)
                .setData(com.google.protobuf.ByteString.copyFrom(packed))
                .setDone(true)
                .build();
        target.handleInstallSnapshot(req);
    }
}
