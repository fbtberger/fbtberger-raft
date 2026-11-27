/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.ClusterConfiguration;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.LogEntry;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One witness on its own, driven through the RPCs a leader would send it. What a witness
 * does in a cluster -- when it votes, when it leads, whom it hands the lead to -- is in
 * {@link WitnessClusterTest}.
 */
class WitnessTest {

    @TempDir
    Path tempDir;

    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<RaftStorage> stores = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(RaftNode::shutdown);
        stores.forEach(RaftStorage::close);
    }

    // ---- no state ----------------------------------------------------------

    @Test
    void aWitnessStoresTheLogAndAppliesNothingToTheStateMachineItWasGiven() throws Exception {
        KeyValueStateMachine given = new KeyValueStateMachine();
        RaftStorage store = wal("w");
        RaftNode witness = node(witnessConfig(1_000, 1_000), store, given);

        assertTrue(witness.handleAppendEntries(batch(1, 5, 0, 5)).getSuccess());

        assertTrue(witness.isWitness());
        assertEquals(5, store.getLastLogIndex(), "the log is what a witness is there to hold");
        assertEquals(5, witness.appliedIndex(), "applied, in the sense that matters to Raft");
        assertNull(given.get("k1"), "and none of it reached a state machine");
    }

    @Test
    void aWitnessRefusesEveryClientRequestAndSaysWhoLeads() throws Exception {
        RaftNode witness = node(witnessConfig(1_000, 1_000), wal("w"), new KeyValueStateMachine());
        assertTrue(witness.handleAppendEntries(batch(1, 2, 0, 2)).getSuccess());

        List<CompletableFuture<?>> refused = List.of(
                witness.submitCommand("SET a 1".getBytes()),
                witness.submitCommand("SET a 1".getBytes(), "client-1", 1, 0),
                witness.query("GET a".getBytes()),
                witness.queryWithLease("GET a".getBytes()),
                witness.queryAnyRole("GET a".getBytes()),
                witness.readIndex(),
                witness.transferLeadership("other1"));

        for (CompletableFuture<?> future : refused) {
            ExecutionException failure =
                    assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
            RaftNode.NotLeaderException notLeader =
                    assertInstanceOf(RaftNode.NotLeaderException.class, failure.getCause());
            assertEquals("leader", notLeader.leaderHint, "a client has to learn where to go instead");
        }
    }

    @Test
    void aWitnessDeclinesToTakeTheLeadOnRequestAndSaysWhy() throws Exception {
        RaftNode witness = node(witnessConfig(1_000, 1_000), wal("w"), new KeyValueStateMachine());
        assertTrue(witness.handleAppendEntries(batch(1, 2, 0, 2)).getSuccess());

        TimeoutNowResponse answer = witness.handleTimeoutNow(
                TimeoutNowRequest.newBuilder().setTerm(1).setLeaderId("leader").build());

        assertTrue(answer.getWitness(), "the leader must be told, or it waits out a timeout");
        assertEquals(ServerRole.FOLLOWER, witness.role());
    }

    @Test
    void aWitnessIsNeverAPlaceToReadFrom() throws Exception {
        RaftStorage store = wal("w");
        RaftNode witness = node(witnessConfig(1_000, 1_000), store, new KeyValueStateMachine());

        HealthCheck.Status serving = new HealthCheck(witness, store).serving();

        assertFalse(serving.ok());
        assertTrue(serving.message().contains("witness"), serving.message());
    }

    // ---- compaction --------------------------------------------------------

    @Test
    void aWitnessCompactsItsLogAndKeepsTheTailItMayHaveToHandOn() throws Exception {
        RaftStorage store = wal("w");
        RaftNode witness = node(witnessConfig(/*threshold*/ 5, /*retain*/ 10), store, new KeyValueStateMachine());

        assertTrue(witness.handleAppendEntries(batch(1, 30, 0, 30)).getSuccess());

        assertEquals(20, witness.snapshotIndex(), "everything but the last ten committed entries");
        assertNull(store.getLogEntry(20));
        assertNotNull(store.getLogEntry(21));
        assertEquals(30, store.getLastLogIndex());
        assertEquals(0, store.getSnapshotMeta().dataLength, "a boundary, not a snapshot of anything");
    }

    @Test
    void aWitnessDoesNotCompactBelowTheThreshold() throws Exception {
        RaftNode witness = node(witnessConfig(/*threshold*/ 50, /*retain*/ 10), wal("w"), new KeyValueStateMachine());

        assertTrue(witness.handleAppendEntries(batch(1, 30, 0, 30)).getSuccess());
        assertEquals(0, witness.snapshotIndex());

        witness.snapshotNow();
        assertEquals(20, witness.snapshotIndex(), "on demand it compacts as far as the tail allows");
    }

    @Test
    void theBoundaryCarriesTheConfigurationAsOfThatIndexNotALaterOne() throws Exception {
        RaftStorage store = wal("w");
        RaftNode witness = node(witnessConfig(5, 10), store, new KeyValueStateMachine());

        // Entry 8 adds n4; entry 25, still in the retained tail, removes it again.
        ClusterConfiguration withN4 = configuration("w", "other1", "other2", "n4");
        ClusterConfiguration withoutN4 = configuration("w", "other1", "other2");
        AppendEntriesRequest.Builder request = AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("leader").setLeaderCommit(30);
        for (int i = 1; i <= 30; i++) {
            LogEntry.Builder entry = LogEntry.newBuilder().setIndex(i).setTerm(1);
            if (i == 8) entry.setConfiguration(withN4);
            else if (i == 25) entry.setConfiguration(withoutN4);
            else entry.setCommand(ByteString.copyFromUtf8("SET k" + i + " " + i));
            request.addEntries(entry);
        }
        assertTrue(witness.handleAppendEntries(request.build()).getSuccess());

        assertEquals(20, witness.snapshotIndex());
        ClusterConfiguration stored = ClusterConfiguration.parseFrom(store.getSnapshotMeta().configurationData);
        assertEquals(4, stored.getMembersCount(), "index 20 lies between the two changes");
        assertFalse(witness.currentConfiguration().containsKey("n4"), "the node itself follows the log");
    }

    @Test
    void aWitnessRestartsFromItsCompactedLog() throws Exception {
        RaftNode witness = node(witnessConfig(5, 10), wal("w"), new KeyValueStateMachine());
        assertTrue(witness.handleAppendEntries(batch(1, 30, 0, 30)).getSuccess());
        closeAll();

        RaftNode again = node(witnessConfig(5, 10), wal("w"), new KeyValueStateMachine());

        assertEquals(20, again.snapshotIndex());
        assertTrue(again.handleAppendEntries(batch(31, 32, 30, 32)).getSuccess(),
                "it must still know the log it kept");
    }

    // ---- snapshots from the leader ----------------------------------------

    @Test
    void aWitnessSentASnapshotKeepsTheBoundaryAndDropsTheState() throws Exception {
        RaftStorage source = wal("leader");
        RaftNode leaderSide = node(dataConfig("leader"), source, new KeyValueStateMachine());
        assertTrue(leaderSide.handleAppendEntries(batch(1, 40, 0, 40)).getSuccess());
        leaderSide.snapshotNow();
        assertTrue(source.getSnapshotMeta().dataLength > 0);

        KeyValueStateMachine given = new KeyValueStateMachine();
        RaftStorage target = wal("w");
        RaftNode witness = node(witnessConfig(1_000, 1_000), target, given);
        send(source, witness, 64);

        assertEquals(40, witness.snapshotIndex());
        assertEquals(40, witness.appliedIndex());
        assertEquals(0, target.getSnapshotMeta().dataLength, "the state must not be stored a third time");
        assertNull(given.get("k1"));
        assertTrue(witness.handleAppendEntries(batch(41, 42, 40, 42)).getSuccess(),
                "and replication continues from the boundary");
    }

    // ---- a witness's directory is not a data node's ------------------------

    @Test
    void aDataNodeRefusesToStartFromADirectoryAWitnessCompacted() throws Exception {
        RaftNode witness = node(witnessConfig(5, 10), wal("shared"), new KeyValueStateMachine());
        assertTrue(witness.handleAppendEntries(batch(1, 30, 0, 30)).getSuccess());
        closeAll();

        // The flag was dropped from the properties file. Entries 1..20 are gone and no
        // state stands in for them: this node would come up with ten keys of thirty.
        IllegalStateException refusal = assertThrows(IllegalStateException.class,
                () -> new RaftNode(dataConfig("w"), wal("shared"), new KeyValueStateMachine(),
                        addr -> null, RaftMetrics.noop()));

        assertTrue(refusal.getMessage().contains("written by a witness"), refusal.getMessage());
        assertTrue(refusal.getMessage().contains("node.witness=true"), refusal.getMessage());
    }

    @Test
    void aWitnessMayTakeOverADataNodesDirectory() throws Exception {
        RaftNode data = node(dataConfig("w"), wal("shared"), new KeyValueStateMachine());
        assertTrue(data.handleAppendEntries(batch(1, 12, 0, 12)).getSuccess());
        data.snapshotNow();
        closeAll();

        RaftNode witness = node(witnessConfig(1_000, 1_000), wal("shared"), new KeyValueStateMachine());

        assertEquals(12, witness.snapshotIndex());
        assertTrue(witness.handleAppendEntries(batch(13, 14, 12, 14)).getSuccess());
    }

    // ---- configuration -----------------------------------------------------

    @Test
    void theWitnessSettingsComeFromThePropertiesFile() throws Exception {
        RaftConfig config = witnessConfig(100, 250);

        assertTrue(config.isWitness());
        assertEquals(250, config.witnessLogRetain());
        assertFalse(dataConfig("w").isWitness());
        assertEquals(RaftConfig.DEFAULT_WITNESS_LOG_RETAIN, dataConfig("w").witnessLogRetain());
    }

    @Test
    void aNodeCannotBeBothAWitnessAndALearner() {
        Properties props = properties("w", 100);
        props.setProperty("node.witness", "true");
        props.setProperty("node.learner", "true");

        IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class, () -> load(props));
        assertTrue(refusal.getMessage().contains("exclude each other"), refusal.getMessage());
    }

    // ---- helpers -----------------------------------------------------------

    private static void send(RaftStorage source, RaftNode follower, int chunkSize) throws Exception {
        RaftNode.SnapshotTransfer transfer = new RaftNode.SnapshotTransfer(source.getSnapshotMeta());
        for (long offset = 0; offset < transfer.totalLength; offset += chunkSize) {
            int length = (int) Math.min(chunkSize, transfer.totalLength - offset);
            follower.handleInstallSnapshot(InstallSnapshotRequest.newBuilder()
                    .setTerm(1).setLeaderId("leader")
                    .setLastIncludedIndex(transfer.lastIncludedIndex)
                    .setLastIncludedTerm(transfer.lastIncludedTerm)
                    .setOffset(offset)
                    .setData(ByteString.copyFrom(transfer.read(source, offset, length)))
                    .setDone(offset + length >= transfer.totalLength)
                    .build());
        }
    }

    private static ClusterConfiguration configuration(String... ids) {
        ClusterConfiguration.Builder builder = ClusterConfiguration.newBuilder();
        for (String id : ids) {
            builder.addMembers(ClusterConfiguration.Member.newBuilder().setId(id).setAddress("localhost:1"));
        }
        return builder.build();
    }

    private static AppendEntriesRequest batch(int from, int to, long prevIdx, long leaderCommit) {
        AppendEntriesRequest.Builder b = AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("leader")
                .setPrevLogIndex(prevIdx).setPrevLogTerm(prevIdx == 0 ? 0 : 1)
                .setLeaderCommit(leaderCommit);
        for (int i = from; i <= to; i++) {
            b.addEntries(LogEntry.newBuilder().setIndex(i).setTerm(1)
                    .setCommand(ByteString.copyFromUtf8("SET k" + i + " " + i)));
        }
        return b.build();
    }

    private RaftStorage wal(String name) {
        RaftStorage store = new WalStorage(tempDir.resolve(name).toFile());
        stores.add(store);
        return store;
    }

    private RaftNode node(RaftConfig config, RaftStorage store, StateMachine sm) {
        RaftNode node = new RaftNode(config, store, sm, addr -> null, RaftMetrics.noop());
        nodes.add(node);
        return node;
    }

    private void closeAll() {
        tearDown();
        nodes.clear();
        stores.clear();
    }

    private RaftConfig witnessConfig(int threshold, int retain) throws Exception {
        Properties props = properties("w", threshold);
        props.setProperty("node.witness", "true");
        props.setProperty("witness.log.retain", Integer.toString(retain));
        return load(props);
    }

    private RaftConfig dataConfig(String selfId) throws Exception {
        return load(properties(selfId, 1_000));
    }

    private Properties properties(String selfId, int threshold) {
        Properties props = new Properties();
        props.setProperty("node.id", selfId);
        props.setProperty("node.port", "9291");
        props.setProperty("data.dir", tempDir.resolve("unused-" + selfId).toString());
        props.setProperty("peer." + selfId, "localhost:9291");
        props.setProperty("peer.other1", "localhost:9292");
        props.setProperty("peer.other2", "localhost:9293");
        props.setProperty("snapshot.threshold", Integer.toString(threshold));
        return props;
    }

    private RaftConfig load(Properties props) throws Exception {
        Path file = Files.createTempFile(tempDir, "raft-witness-", ".properties");
        try (OutputStream out = Files.newOutputStream(file)) {
            props.store(out, null);
        }
        return RaftConfig.load(file);
    }
}
