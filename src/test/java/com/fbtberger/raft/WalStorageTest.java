package com.fbtberger.raft;

import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WalStorageTest {

    @TempDir File tempDir;
    private WalStorage store;

    @BeforeEach
    void setUp() {
        store = new WalStorage(tempDir);
    }

    @AfterEach
    void tearDown() {
        store.close();
    }

    @Test
    void initialStateIsAllZero() {
        assertEquals(0, store.getCurrentTerm());
        assertNull(store.getVotedFor());
        assertEquals(0, store.getLastLogIndex());
        assertEquals(0, store.getLastLogTerm());
        assertEquals(0, store.getSnapshotIndex());
        assertNull(store.getSnapshot());
    }

    @Test
    void termAndVoteAreStoredTogether() {
        store.setTermAndVote(3, "node1");
        assertEquals(3, store.getCurrentTerm());
        assertEquals("node1", store.getVotedFor());
    }

    @Test
    void votedForCanBeCleared() {
        store.setTermAndVote(1, "node1");
        store.setTermAndVote(2, null);
        assertNull(store.getVotedFor());
        assertEquals(2, store.getCurrentTerm());
    }

    @Test
    void appendedEntriesAreRetrievableByIndex() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        assertNotNull(store.getLogEntry(1));
        assertNotNull(store.getLogEntry(2));
        assertEquals(1, store.getLogEntry(1).getIndex());
    }

    @Test
    void lastLogIndexAndTermReflectMostRecentEntry() {
        store.appendEntries(List.of(entry(1, 1, "x"), entry(2, 2, "y")));
        assertEquals(2, store.getLastLogIndex());
        assertEquals(2, store.getLastLogTerm());
    }

    @Test
    void missingEntryReturnsNull() {
        assertNull(store.getLogEntry(99));
    }

    @Test
    void truncateRemovesEntriesAtAndAfterGivenIndex() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")));
        store.truncateFrom(2);
        assertNull(store.getLogEntry(2));
        assertNull(store.getLogEntry(3));
        assertNotNull(store.getLogEntry(1));
        assertEquals(1, store.getLastLogIndex());
    }

    @Test
    void truncatingFromOneClears() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        store.truncateFrom(1);
        assertEquals(0, store.getLastLogIndex());
        assertEquals(0, store.getLastLogTerm());
    }

    @Test
    void getTermAtZeroIsZero() { assertEquals(0, store.getTermAt(0)); }

    @Test
    void getTermAtReturnsMinus1ForUnknownIndex() { assertEquals(-1, store.getTermAt(7)); }

    @Test
    void getTermAtReturnsTermOfPresentEntry() {
        store.appendEntries(List.of(entry(1, 3, "x")));
        assertEquals(3, store.getTermAt(1));
    }

    @Test
    void savingSnapshotUpdatesSnapshotBoundary() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 2, "c")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 1, new byte[]{1, 2}, new byte[]{9}));
        assertEquals(2, store.getSnapshotIndex());
        assertEquals(1, store.getSnapshotTerm());
    }

    @Test
    void savingSnapshotCompactsLogUpToAndIncludingBoundary() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 2, "c")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 1, new byte[0], new byte[0]));
        assertNull(store.getLogEntry(1));
        assertNull(store.getLogEntry(2));
        assertNotNull(store.getLogEntry(3));
    }

    @Test
    void lastLogIndexFallsBackToSnapshotWhenLogIsFullyCompacted() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 2, "b")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 2, new byte[0], new byte[0]));
        assertEquals(2, store.getLastLogIndex());
        assertEquals(2, store.getLastLogTerm());
    }

    @Test
    void getSnapshotReturnsSavedPayload() {
        byte[] smData = "hello".getBytes();
        byte[] cfgData = "cfg".getBytes();
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(5, 3, smData, cfgData));
        RaftStorage.Snapshot snap = store.getSnapshot();
        assertNotNull(snap);
        assertEquals(5, snap.lastIncludedIndex);
        assertArrayEquals(smData, snap.stateMachineData);
        assertArrayEquals(cfgData, snap.configurationData);
    }

    @Test
    void getTermAtSnapshotBoundaryReturnsSnapshotTerm() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 1, new byte[0], new byte[0]));
        assertEquals(1, store.getTermAt(2));
    }

    // ---- WAL recovery ----------------------------------------------------

    @Test
    void recoverMetaAfterReopen() {
        store.setTermAndVote(7, "nodeX");
        store.close();

        store = new WalStorage(tempDir);
        assertEquals(7, store.getCurrentTerm());
        assertEquals("nodeX", store.getVotedFor());
    }

    @Test
    void recoverLogAfterReopen() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 2, "b")));
        store.close();

        store = new WalStorage(tempDir);
        assertEquals(2, store.getLastLogIndex());
        assertEquals(2, store.getLastLogTerm());
        assertNotNull(store.getLogEntry(1));
        assertNotNull(store.getLogEntry(2));
        assertEquals("a", store.getLogEntry(1).getCommand().toStringUtf8());
    }

    @Test
    void recoverSnapshotAfterReopen() {
        byte[] smData = "state".getBytes();
        byte[] cfgData = "cfg".getBytes();
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(10, 5, smData, cfgData));
        store.close();

        store = new WalStorage(tempDir);
        assertEquals(10, store.getSnapshotIndex());
        assertEquals(5, store.getSnapshotTerm());
        assertArrayEquals(smData, store.getSnapshot().stateMachineData);
    }

    @Test
    void recoverAfterSnapshotAndSubsequentAppend() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(1, 1, new byte[0], new byte[0]));
        store.appendEntries(List.of(entry(3, 2, "c")));
        store.close();

        store = new WalStorage(tempDir);
        assertEquals(3, store.getLastLogIndex());
        assertNull(store.getLogEntry(1));
        assertNotNull(store.getLogEntry(2));
        assertNotNull(store.getLogEntry(3));
    }

    @Test
    void truncateAfterSnapshotPreservesSnapshotBoundary() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(1, 1, new byte[0], new byte[0]));
        store.truncateFrom(2);
        assertEquals(1, store.getLastLogIndex());
        assertEquals(1, store.getLastLogTerm());
        assertEquals(1, store.getSnapshotIndex());
    }

    @Test
    void appendAfterTruncateWorks() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        store.truncateFrom(2);
        store.appendEntries(List.of(entry(2, 2, "b2")));
        assertEquals(2, store.getLastLogIndex());
        assertEquals(2, store.getLastLogTerm());
        assertEquals("b2", store.getLogEntry(2).getCommand().toStringUtf8());
    }

    @Test
    void getTermAtReturnsMinus1ForCompactedEntry() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 1, new byte[0], new byte[0]));
        assertEquals(-1, store.getTermAt(1));
    }

    @Test
    void deferSyncReturnsCompletableFuture() throws Exception {
        var future = store.appendEntriesDeferSync(List.of(entry(1, 1, "a")));
        assertNotNull(future);
        future.get(2, java.util.concurrent.TimeUnit.SECONDS);
        assertEquals(1, store.getLastLogIndex());
    }

    private static LogEntry entry(long index, long term, String command) {
        return LogEntry.newBuilder()
                .setIndex(index).setTerm(term)
                .setCommand(ByteString.copyFromUtf8(command))
                .build();
    }
}
