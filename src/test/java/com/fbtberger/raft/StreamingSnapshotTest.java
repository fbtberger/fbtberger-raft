/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A snapshot is never one array: not when it is taken, not when it is stored, not when it
 * is sent, not when it is received and not when it is restored.
 *
 * <p>The state machine used here makes that checkable. Its array methods throw, so a node
 * that still called {@code takeSnapshot()}, {@code prepareCowSnapshot()} or
 * {@code restoreSnapshot(byte[])} anywhere on these paths fails the test instead of quietly
 * holding the whole state in memory. What the state machine cannot see -- the storage
 * backend and the transfer -- is covered by sending the snapshot in chunks of five bytes,
 * which cuts through the four-byte header, the payload and the trailer.
 */
class StreamingSnapshotTest {

    @TempDir
    Path tempDir;

    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<RaftStorage> stores = new ArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.forEach(RaftNode::shutdown);
        stores.forEach(RaftStorage::close);
    }

    @Test
    void aBackgroundSnapshotIsWrittenAndRestoredWithoutTheArrayMethods() throws Exception {
        StreamOnlyStateMachine sm = new StreamOnlyStateMachine();
        RaftNode node = node("a", wal("a"), sm, /*threshold*/ 5);

        assertTrue(node.handleAppendEntries(batch(1, 8, 0, 8)).getSuccess());
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.snapshotIndex() == 8);

        // A restart on the same directory: the state has to come back out of the file.
        closeAll();
        StreamOnlyStateMachine restarted = new StreamOnlyStateMachine();
        RaftNode again = node("a", wal("a"), restarted, 5);

        assertEquals(8, again.appliedIndex());
        assertEquals("8", restarted.get("k8"));
        assertEquals("1", restarted.get("k1"));
    }

    @Test
    void aSnapshotOnDemandIsWrittenWithoutTheArrayMethods() throws Exception {
        StreamOnlyStateMachine sm = new StreamOnlyStateMachine();
        RaftStorage store = wal("a");
        RaftNode node = node("a", store, sm, /*threshold*/ 1_000);

        assertTrue(node.handleAppendEntries(batch(1, 3, 0, 3)).getSuccess());
        node.snapshotNow();

        assertEquals(3, node.snapshotIndex());
        StreamOnlyStateMachine copy = new StreamOnlyStateMachine();
        try (InputStream in = store.openSnapshotData()) {
            copy.restoreSnapshot(in);
        }
        assertEquals("3", copy.get("k3"));
    }

    @Test
    void aSnapshotTravelsFromOneBackendToAnotherInSmallChunks() throws Exception {
        RaftStorage source = wal("leader");
        RaftNode leaderSide = node("leader", source, new StreamOnlyStateMachine(), 1_000);
        assertTrue(leaderSide.handleAppendEntries(taggedBatch(1, 40, 0, 40)).getSuccess());
        leaderSide.snapshotNow();
        assertEquals(1, leaderSide.clientSessionCount());

        // Berkeley DB on the receiving side: what is sent does not depend on how it is stored.
        StreamOnlyStateMachine received = new StreamOnlyStateMachine();
        File followerDir = tempDir.resolve("follower").toFile();
        RaftStorage target = track(new BerkeleyDbStorage(followerDir));
        RaftNode follower = node("follower", target, received, 1_000);

        int chunks = send(source, follower, /*chunkSize*/ 5);

        assertTrue(chunks > 50, "the test is only worth something if the stream was really cut up");
        assertEquals(40, follower.snapshotIndex());
        assertEquals(40, follower.appliedIndex());
        assertEquals("40", received.get("k40"));
        assertEquals("1", received.get("k1"));
        assertEquals(1, follower.clientSessionCount(), "the session table travels with the state");
        assertEquals(source.getSnapshotMeta().dataLength, target.getSnapshotMeta().dataLength);
        assertNoTemporaryFiles(followerDir);
    }

    @Test
    void aTransferThatBreaksOffLeavesNothingBehindAndTheNextOneStartsClean() throws Exception {
        RaftStorage source = wal("leader");
        RaftNode leaderSide = node("leader", source, new StreamOnlyStateMachine(), 1_000);
        assertTrue(leaderSide.handleAppendEntries(batch(1, 40, 0, 40)).getSuccess());
        leaderSide.snapshotNow();

        StreamOnlyStateMachine received = new StreamOnlyStateMachine();
        File followerDir = tempDir.resolve("follower").toFile();
        RaftStorage target = track(new WalStorage(followerDir));
        RaftNode follower = node("follower", target, received, 1_000);

        // Half of it arrives, then a chunk from somewhere further on: the leader lost its
        // place, or the connection lost a chunk.
        RaftNode.SnapshotTransfer transfer = new RaftNode.SnapshotTransfer(source.getSnapshotMeta());
        follower.handleInstallSnapshot(chunk(transfer, source, 0, 64));
        follower.handleInstallSnapshot(chunk(transfer, source, 64, 64));
        follower.handleInstallSnapshot(chunk(transfer, source, 400, 64));

        assertEquals(0, follower.snapshotIndex(), "half a snapshot is no snapshot");
        assertNull(received.get("k1"), "and nothing of it reached the state machine");
        assertNoTemporaryFiles(followerDir);

        send(source, follower, 64);

        assertEquals(40, follower.snapshotIndex());
        assertEquals("40", received.get("k40"));
        assertNoTemporaryFiles(followerDir);
    }

    @Test
    void aFollowerRestartedAfterReceivingASnapshotRestoresItFromItsOwnFile() throws Exception {
        RaftStorage source = wal("leader");
        RaftNode leaderSide = node("leader", source, new StreamOnlyStateMachine(), 1_000);
        assertTrue(leaderSide.handleAppendEntries(batch(1, 12, 0, 12)).getSuccess());
        leaderSide.snapshotNow();

        File followerDir = tempDir.resolve("follower").toFile();
        RaftNode follower = node("follower", track(new BerkeleyDbStorage(followerDir)),
                new StreamOnlyStateMachine(), 1_000);
        send(source, follower, 32);
        closeAll();

        StreamOnlyStateMachine restarted = new StreamOnlyStateMachine();
        RaftNode again = node("follower", track(new BerkeleyDbStorage(followerDir)), restarted, 1_000);

        assertEquals(12, again.appliedIndex());
        assertEquals("12", restarted.get("k12"));
    }

    // ---- helpers ---------------------------------------------------------

    /** Sends the stored snapshot of {@code source} the way a leader does; returns the number of chunks. */
    private static int send(RaftStorage source, RaftNode follower, int chunkSize) throws IOException {
        RaftNode.SnapshotTransfer transfer = new RaftNode.SnapshotTransfer(source.getSnapshotMeta());
        int chunks = 0;
        for (long offset = 0; offset < transfer.totalLength; offset += chunkSize) {
            follower.handleInstallSnapshot(chunk(transfer, source, offset, chunkSize));
            chunks++;
        }
        return chunks;
    }

    private static InstallSnapshotRequest chunk(RaftNode.SnapshotTransfer transfer, RaftStorage source,
                                                long offset, int chunkSize) throws IOException {
        int length = (int) Math.min(chunkSize, transfer.totalLength - offset);
        return InstallSnapshotRequest.newBuilder()
                .setTerm(1).setLeaderId("leader")
                .setLastIncludedIndex(transfer.lastIncludedIndex)
                .setLastIncludedTerm(transfer.lastIncludedTerm)
                .setOffset(offset)
                .setData(ByteString.copyFrom(transfer.read(source, offset, length)))
                .setDone(offset + length >= transfer.totalLength)
                .build();
    }

    private static void assertNoTemporaryFiles(File dir) throws IOException {
        try (var files = Files.walk(dir.toPath())) {
            List<String> leftovers = files.map(p -> p.getFileName().toString())
                    .filter(name -> name.contains("tmp")).toList();
            assertTrue(leftovers.isEmpty(), "temporary snapshot files left behind: " + leftovers);
        }
    }

    private RaftStorage wal(String name) {
        return track(new WalStorage(tempDir.resolve(name).toFile()));
    }

    private RaftStorage track(RaftStorage store) {
        stores.add(store);
        return store;
    }

    private RaftNode node(String id, RaftStorage store, StateMachine sm, int threshold) throws Exception {
        RaftNode node = new RaftNode(config(id, threshold), store, sm, addr -> null, RaftMetrics.noop());
        nodes.add(node);
        return node;
    }

    private void closeAll() {
        tearDown();
        nodes.clear();
        stores.clear();
    }

    private static AppendEntriesRequest batch(int from, int to, long prevIdx, long leaderCommit) {
        return batch(from, to, prevIdx, leaderCommit, false);
    }

    /** As {@link #batch}, with every entry tagged as one client's request, so a session table exists. */
    private static AppendEntriesRequest taggedBatch(int from, int to, long prevIdx, long leaderCommit) {
        return batch(from, to, prevIdx, leaderCommit, true);
    }

    private static AppendEntriesRequest batch(int from, int to, long prevIdx, long leaderCommit, boolean tagged) {
        AppendEntriesRequest.Builder b = AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("leader")
                .setPrevLogIndex(prevIdx).setPrevLogTerm(prevIdx == 0 ? 0 : 1)
                .setLeaderCommit(leaderCommit);
        for (int i = from; i <= to; i++) {
            LogEntry.Builder entry = LogEntry.newBuilder().setIndex(i).setTerm(1)
                    .setCommand(ByteString.copyFromUtf8("SET k" + i + " " + i));
            if (tagged) {
                entry.setClientId("client-1").setSequence(i);
            }
            b.addEntries(entry);
        }
        return b.build();
    }

    private RaftConfig config(String selfId, int threshold) throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", selfId);
        props.setProperty("node.port", "9191");
        props.setProperty("data.dir", tempDir.resolve("unused-" + selfId).toString());
        props.setProperty("peer." + selfId, "localhost:9191");
        props.setProperty("peer.other1", "localhost:9192");
        props.setProperty("peer.other2", "localhost:9193");
        props.setProperty("snapshot.threshold", Integer.toString(threshold));
        Path file = Files.createTempFile(tempDir, "raft-streaming-", ".properties");
        try (OutputStream out = Files.newOutputStream(file)) {
            props.store(out, null);
        }
        return RaftConfig.load(file);
    }

    /** A key-value store that can only be snapshotted and restored as a stream. */
    private static final class StreamOnlyStateMachine implements StateMachine {
        private final KeyValueStateMachine delegate = new KeyValueStateMachine();

        String get(String key) {
            return delegate.get(key);
        }

        @Override
        public byte[] apply(byte[] command) {
            return delegate.apply(command);
        }

        @Override
        public void writeSnapshot(OutputStream out) throws IOException {
            delegate.writeSnapshot(out);
        }

        @Override
        public SnapshotWriter prepareSnapshotWriter() {
            return delegate.prepareSnapshotWriter();
        }

        @Override
        public void restoreSnapshot(InputStream in) throws IOException {
            delegate.restoreSnapshot(in);
        }

        @Override
        public byte[] takeSnapshot() {
            throw new AssertionError("the whole state was asked for as one array");
        }

        @Override
        public Supplier<byte[]> prepareCowSnapshot() {
            throw new AssertionError("the whole state was asked for as one array");
        }

        @Override
        public void restoreSnapshot(byte[] snapshot) {
            throw new AssertionError("the whole state was handed over as one array");
        }
    }
}
