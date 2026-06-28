/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link InMemoryStorage}: the non-durable RaftStorage
 * implementation used in tests and quick demos. Every invariant here
 * applies equally to {@link BerkeleyDbStorage} (same interface, same
 * expected behaviour) -- these tests just run without needing a real
 * Berkeley DB environment on disk.
 */
class InMemoryStorageTest {

    private InMemoryStorage store;

    @BeforeEach
    void setUp() {
        store = new InMemoryStorage();
    }

    // ---- initial state --------------------------------------------------

    @Test
    void initialStateIsAllZero() {
        assertEquals(0, store.getCurrentTerm());
        assertNull(store.getVotedFor());
        assertEquals(0, store.getLastLogIndex());
        assertEquals(0, store.getLastLogTerm());
        assertEquals(0, store.getSnapshotIndex());
        assertEquals(0, store.getSnapshotTerm());
        assertNull(store.getSnapshot());
    }

    // ---- term / vote ----------------------------------------------------

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

    // ---- log append / lookup --------------------------------------------

    @Test
    void appendedEntriesAreRetrievableByIndex() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        LogEntry e1 = store.getLogEntry(1);
        LogEntry e2 = store.getLogEntry(2);
        assertNotNull(e1);
        assertNotNull(e2);
        assertEquals(1, e1.getIndex());
        assertEquals(2, e2.getIndex());
        assertEquals(1, e1.getTerm());
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

    // ---- truncation ------------------------------------------------------

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

    // ---- getTermAt ------------------------------------------------------

    @Test
    void getTermAtZeroIsZero() {
        assertEquals(0, store.getTermAt(0));
    }

    @Test
    void getTermAtReturnsMinus1ForUnknownIndex() {
        assertEquals(-1, store.getTermAt(7));
    }

    @Test
    void getTermAtReturnsTermOfPresentEntry() {
        store.appendEntries(List.of(entry(1, 3, "x")));
        assertEquals(3, store.getTermAt(1));
    }

    @Test
    void getTermAtSnapshotBoundaryReturnsSnapshotTerm() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        RaftStorage.Snapshot snap = new RaftStorage.Snapshot(2, 1, new byte[]{42}, new byte[0]);
        store.saveSnapshotAndCompact(snap);
        assertEquals(1, store.getTermAt(2)); // boundary of the snapshot
    }

    // ---- snapshot + compaction ------------------------------------------

    @Test
    void savingSnapshotUpdatesSnapshotBoundary() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 2, "c")));
        byte[] smData = {1, 2, 3};
        byte[] cfgData = {9};
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 1, smData, cfgData));

        assertEquals(2, store.getSnapshotIndex());
        assertEquals(1, store.getSnapshotTerm());
    }

    @Test
    void savingSnapshotCompactsLogUpToAndIncludingBoundary() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 2, "c")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 1, new byte[0], new byte[0]));

        assertNull(store.getLogEntry(1));
        assertNull(store.getLogEntry(2));
        assertNotNull(store.getLogEntry(3)); // after boundary -- preserved
    }

    @Test
    void lastLogIndexFallsBackToSnapshotWhenLogIsFullyCompacted() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 2, "b")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 2, new byte[0], new byte[0]));

        assertEquals(2, store.getLastLogIndex());
        assertEquals(2, store.getLastLogTerm());
    }

    @Test
    void getSnapshotReturnsNullBeforeAnySnapshot() {
        assertNull(store.getSnapshot());
    }

    @Test
    void getSnapshotReturnsSavedPayload() {
        byte[] smData = "hello".getBytes();
        byte[] cfgData = "cfg".getBytes();
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(5, 3, smData, cfgData));
        RaftStorage.Snapshot snap = store.getSnapshot();
        assertNotNull(snap);
        assertEquals(5, snap.lastIncludedIndex);
        assertEquals(3, snap.lastIncludedTerm);
        assertArrayEquals(smData, snap.stateMachineData);
        assertArrayEquals(cfgData, snap.configurationData);
    }

    @Test
    void getTermAtReturnsMinus1ForCompactedEntry() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 1, new byte[0], new byte[0]));
        // Index 1 is now compacted away, NOT at the boundary; must return -1
        assertEquals(-1, store.getTermAt(1));
    }

    // ---- helpers --------------------------------------------------------

    private static LogEntry entry(long index, long term, String command) {
        return LogEntry.newBuilder()
                .setIndex(index)
                .setTerm(term)
                .setCommand(ByteString.copyFromUtf8(command))
                .build();
    }
}
