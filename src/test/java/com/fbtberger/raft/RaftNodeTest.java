package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.ClusterConfiguration;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.LogEntry;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
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

    private InMemoryStorage store;
    private KeyValueStateMachine sm;
    private RaftNode node;

    // A single-node cluster: selfId = "n1", peers = just "n1".
    private static final Map<String, String> SELF_ONLY = Map.of("n1", "localhost:9091");

    @BeforeEach
    void setUp() throws Exception {
        store = new InMemoryStorage();
        sm = new KeyValueStateMachine();
        RaftConfig config = singleNodeConfig();
        node = new RaftNode(config, store, sm, address -> null); // no peers -> no stubs needed
        node.start();
        // Give the election timer a moment to fire: a single-node cluster
        // needs exactly one vote (its own) and immediately becomes leader.
        awaitLeader(1_000);
    }

    @AfterEach
    void tearDown() {
        node.shutdown();
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
    void followerRejectsSubmitWithNotLeaderException() {
        // Manually demote to FOLLOWER via a higher-term RequestVote, which
        // forces becomeFollower. We do this by delivering an AppendEntries
        // with term=99, which is a valid way to step down.
        AppendEntriesRequest ae = AppendEntriesRequest.newBuilder()
                .setTerm(99).setLeaderId("other").setPrevLogIndex(0).setPrevLogTerm(0)
                .setLeaderCommit(0).build();
        node.handleAppendEntries(ae); // node steps down to term 99 as follower

        CompletableFuture<byte[]> future = node.submitCommand("SET x 1".getBytes(StandardCharsets.UTF_8));
        assertTrue(future.isCompletedExceptionally());
        assertThrows(Exception.class, () -> future.get(100, TimeUnit.MILLISECONDS));
    }

    // ---- cluster reconfiguration (§6) -----------------------------------

    @Test
    void addServerAppendsConfigurationEntry() throws Exception {
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

        // Step our node down so it accepts the RPC
        AppendEntriesRequest ae = AppendEntriesRequest.newBuilder()
                .setTerm(99).setLeaderId("other").setPrevLogIndex(0).setPrevLogTerm(0)
                .setLeaderCommit(0).build();
        node.handleAppendEntries(ae);

        InstallSnapshotRequest req = InstallSnapshotRequest.newBuilder()
                .setTerm(99).setLeaderId("other")
                .setLastIncludedIndex(10).setLastIncludedTerm(99)
                .setStateMachineData(com.google.protobuf.ByteString.copyFrom(smData))
                .setConfigurationData(com.google.protobuf.ByteString.copyFrom(cfgData))
                .build();

        InstallSnapshotResponse resp = node.handleInstallSnapshot(req);
        assertEquals(99, resp.getTerm()); // our currentTerm after stepping down

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
        InstallSnapshotRequest stale = InstallSnapshotRequest.newBuilder()
                .setTerm(store.getCurrentTerm())
                .setLeaderId("n1")
                .setLastIncludedIndex(localSnap - 1) // strictly older
                .setLastIncludedTerm(1)
                .setStateMachineData(com.google.protobuf.ByteString.EMPTY)
                .setConfigurationData(com.google.protobuf.ByteString.EMPTY)
                .build();

        node.handleInstallSnapshot(stale);
        assertEquals(localSnap, node.snapshotIndex(), "stale snapshot must not regress our boundary");
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

    // ---- helpers --------------------------------------------------------

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
}
