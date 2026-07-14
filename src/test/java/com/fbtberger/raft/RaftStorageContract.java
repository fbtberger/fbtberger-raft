/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The invariants that <b>every</b> {@link RaftStorage} implementation must satisfy, run against
 * every implementation. Subclass this once per implementation and supply a factory; that is the
 * whole contract.
 *
 * <h2>Why this class exists</h2>
 * There used to be three storage test classes -- one per implementation -- and they had quietly
 * drifted apart. {@code InMemoryStorageTest} tested that a snapshot boundary never moves backwards;
 * the other two did not. {@code WalStorageTest} tested truncation after a snapshot and the
 * deferred-sync path; the other two did not. And {@code BerkeleyDbStorage} had, for most of its
 * life, no tests at all -- while {@code InMemoryStorageTest} asserted in its own javadoc that
 * "every invariant here applies equally to BerkeleyDbStorage (same interface, same expected
 * behaviour)".
 *
 * <p>That claim was false, and the falsehood was expensive. {@link BerkeleyDbStorage#truncateFrom}
 * committed its transaction while a cursor was still open; Berkeley DB rejects that outright, so
 * <b>every truncate failed, always</b> -- behaviour the in-memory implementation cannot even
 * express, having neither transactions nor cursors. {@code truncateFrom} is reached only by a
 * follower that has to catch up (AppendEntries rule 3), so a cluster that never loses a node never
 * touches the path. When dev finally did lose nodes, three of five sat with an empty state machine
 * for hours while the cluster reported itself healthy: the surviving two were still a majority.
 *
 * <p>The lesson is not "write a test for Berkeley DB". It is that an interface with several
 * implementations needs <b>one</b> set of invariants, enforced against all of them -- so that
 * "passes in memory" can never again be mistaken for "passes on disk".
 *
 * <h2>Durability</h2>
 * {@link InMemoryStorage} deliberately does not persist anything, so the durability invariants
 * (Figure 2: currentTerm, votedFor and the log must survive a crash) cannot hold for it. Those
 * tests call {@link #reopen()}, which skips the test for non-durable implementations rather than
 * pretending they passed.
 */
abstract class RaftStorageContract {

    @TempDir
    Path tempDir;

    protected RaftStorage store;

    /**
     * Opens a store backed by {@code dir}. Called again with the same {@code dir} by
     * {@link #reopen()}, so a durable implementation must find its state where it left it.
     */
    protected abstract RaftStorage create(Path dir);

    /**
     * Whether this implementation is required to survive a close/reopen. Only
     * {@link InMemoryStorage} may answer {@code false}, and only because it documents itself as
     * unsafe to run a real cluster on.
     */
    protected abstract boolean isDurable();

    @BeforeEach
    void openStore() {
        store = create(tempDir);
    }

    @AfterEach
    void closeStore() {
        if (store != null) store.close();
    }

    /** Closes and reopens the store; aborts the calling test if the implementation isn't durable. */
    protected void reopen() {
        assumeTrue(isDurable(), "not durable by design -- the durability invariants do not apply");
        store.close();
        store = create(tempDir);
    }

    // ---- initial state ---------------------------------------------------

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

    // ---- term / vote (Figure 2) ------------------------------------------

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
        assertEquals(2, store.getCurrentTerm());
        assertNull(store.getVotedFor());
    }

    @Test
    void termAndVoteSurviveAReopen() {
        // §5.2: a server that forgets its vote may vote twice in the same term.
        store.setTermAndVote(7, "kwatro2");
        reopen();

        assertEquals(7, store.getCurrentTerm());
        assertEquals("kwatro2", store.getVotedFor());
    }

    // ---- log append / lookup ---------------------------------------------

    @Test
    void appendedEntriesAreRetrievableByIndex() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));

        assertNotNull(store.getLogEntry(1));
        assertEquals(1, store.getLogEntry(1).getIndex());
        assertEquals(1, store.getLogEntry(1).getTerm());
        assertEquals("b", store.getLogEntry(2).getCommand().toStringUtf8());
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
    void theLogSurvivesAReopen() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 3, "b")));
        reopen();

        assertEquals(2, store.getLastLogIndex());
        assertEquals(3, store.getLastLogTerm());
        assertEquals("a", store.getLogEntry(1).getCommand().toStringUtf8());
    }

    // ---- truncation (AppendEntries rule 3) -------------------------------
    // This is the block that would have caught the BerkeleyDbStorage cursor bug.

    @Test
    void truncateRemovesTheEntryAtTheIndexAndEverythingAfterIt() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")));

        store.truncateFrom(2);

        assertNotNull(store.getLogEntry(1));
        assertNull(store.getLogEntry(2));
        assertNull(store.getLogEntry(3));
        assertEquals(1, store.getLastLogIndex());
        assertEquals(1, store.getLastLogTerm());
    }

    @Test
    void truncatingFromOneEmptiesTheLog() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));

        store.truncateFrom(1);

        assertEquals(0, store.getLastLogIndex());
        assertEquals(0, store.getLastLogTerm());
        assertNull(store.getLogEntry(1));
    }

    @Test
    void truncateIsIdempotentAndToleratesAnIndexBeyondTheLog() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));

        store.truncateFrom(5);                     // nothing there -- must not throw
        assertEquals(2, store.getLastLogIndex());

        store.truncateFrom(1);
        assertEquals(0, store.getLastLogIndex());

        store.truncateFrom(1);                     // again, on an empty log
        assertEquals(0, store.getLastLogIndex());
    }

    @Test
    void truncateThenAppendIsHowAFollowerAdoptsTheLeadersLog() {
        // AppendEntries rules 3 and 4: discard the conflicting suffix, then take the leader's
        // version. The exact sequence that was failing in production.
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")));

        store.truncateFrom(2);
        store.appendEntries(List.of(entry(2, 5, "b2"), entry(3, 5, "c2"), entry(4, 5, "d2")));

        assertEquals(4, store.getLastLogIndex());
        assertEquals(5, store.getLastLogTerm());
        assertEquals("b2", store.getLogEntry(2).getCommand().toStringUtf8());
        assertEquals(5, store.getTermAt(3));
    }

    @Test
    void truncateHandlesALogLargerThanASingleCursorStep() {
        // The Berkeley DB failure was in a cursor loop deleting many entries under one
        // transaction. A three-entry log is a weak probe for that; make it walk.
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 1; i <= 200; i++) {
            entries.add(entry(i, 1 + i / 50, "cmd-" + i));
        }
        store.appendEntries(entries);
        assertEquals(200, store.getLastLogIndex());

        store.truncateFrom(51);

        assertEquals(50, store.getLastLogIndex());
        assertNotNull(store.getLogEntry(50));
        assertNull(store.getLogEntry(51));
        assertNull(store.getLogEntry(200));
    }

    @Test
    void repeatedTruncateAndAppendCyclesConverge() {
        // When truncateFrom threw, the leader reset nextIndex to 1 and resent the whole log --
        // and the follower threw again, forever. A follower must be able to run this loop.
        for (int round = 1; round <= 5; round++) {
            store.truncateFrom(1);
            List<LogEntry> entries = new ArrayList<>();
            for (int i = 1; i <= 20; i++) {
                entries.add(entry(i, round, "r" + round + "-" + i));
            }
            store.appendEntries(entries);

            assertEquals(20, store.getLastLogIndex(), "round " + round);
            assertEquals(round, store.getLastLogTerm(), "round " + round);
            assertEquals("r" + round + "-7", store.getLogEntry(7).getCommand().toStringUtf8());
        }
    }

    @Test
    void aTruncateSurvivesAReopen() {
        // Was the deletion really committed, or only reflected in a cached bound?
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c")));
        store.truncateFrom(2);
        reopen();

        assertEquals(1, store.getLastLogIndex());
        assertEquals(1, store.getLastLogTerm());
        assertNull(store.getLogEntry(2));
        assertNull(store.getLogEntry(3));
    }

    @Test
    void truncateAfterASnapshotFallsBackToTheSnapshotBoundary() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 2, "c")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 1, new byte[0], new byte[0]));

        store.truncateFrom(3);   // discard everything the snapshot doesn't cover

        assertEquals(2, store.getLastLogIndex());
        assertEquals(1, store.getLastLogTerm());
        assertEquals(2, store.getSnapshotIndex());
    }

    // ---- deferred sync (§10.2.1) -----------------------------------------

    @Test
    void deferredSyncEntriesAreImmediatelyReadableAndEventuallyDurable() throws Exception {
        store.appendEntriesDeferSync(List.of(entry(1, 1, "a"), entry(2, 1, "b")))
                .get(5, TimeUnit.SECONDS);

        assertEquals(2, store.getLastLogIndex());
        assertEquals("a", store.getLogEntry(1).getCommand().toStringUtf8());

        reopen();
        assertEquals(2, store.getLastLogIndex());
        assertEquals("b", store.getLogEntry(2).getCommand().toStringUtf8());
    }

    // ---- getTermAt -------------------------------------------------------

    @Test
    void getTermAtZeroIsZero() {
        assertEquals(0, store.getTermAt(0));
    }

    @Test
    void getTermAtAnUnknownIndexIsMinusOne() {
        assertEquals(-1, store.getTermAt(7));
    }

    @Test
    void getTermAtReturnsTheTermOfAPresentEntry() {
        store.appendEntries(List.of(entry(1, 3, "x")));
        assertEquals(3, store.getTermAt(1));
    }

    @Test
    void getTermAtTheSnapshotBoundaryReturnsTheSnapshotTerm() {
        // §7: the boundary term must stay known even though the entry itself is gone, or a
        // follower can never have its prevLogTerm check succeed there again.
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 1, new byte[]{42}, new byte[0]));

        assertEquals(1, store.getTermAt(2));
    }

    @Test
    void getTermAtACompactedEntryIsMinusOne() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 1, new byte[0], new byte[0]));

        assertEquals(-1, store.getTermAt(1), "index 1 is below the boundary, not at it");
    }

    @Test
    void getTermAtATruncatedEntryIsMinusOne() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        store.truncateFrom(2);

        assertEquals(-1, store.getTermAt(2));
    }

    // ---- snapshot + compaction (§7) --------------------------------------

    @Test
    void savingASnapshotUpdatesTheBoundary() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 2, "c")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 1, new byte[]{1, 2, 3}, new byte[]{9}));

        assertEquals(2, store.getSnapshotIndex());
        assertEquals(1, store.getSnapshotTerm());
    }

    @Test
    void savingASnapshotCompactsTheLogUpToAndIncludingTheBoundary() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 2, "c")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 1, new byte[0], new byte[0]));

        assertNull(store.getLogEntry(1));
        assertNull(store.getLogEntry(2));
        assertNotNull(store.getLogEntry(3), "entries past the boundary must be left alone");
        assertEquals(3, store.getLastLogIndex());
    }

    @Test
    void lastLogIndexFallsBackToTheSnapshotWhenTheLogIsFullyCompacted() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 2, "b")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 2, new byte[0], new byte[0]));

        assertEquals(2, store.getLastLogIndex());
        assertEquals(2, store.getLastLogTerm());
    }

    @Test
    void getSnapshotIsNullBeforeAnySnapshot() {
        assertNull(store.getSnapshot());
    }

    @Test
    void getSnapshotReturnsTheSavedPayload() {
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
    void theSnapshotBoundaryNeverMovesBackwards() {
        // A background (COW) snapshot in RaftNode decides to save off-lock. If a newer snapshot --
        // e.g. one installed by InstallSnapshot after a step-down -- landed in between, the stale
        // save must be dropped, not allowed to rewind compaction.
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(10, 5, new byte[]{1}, new byte[0]));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(3, 2, new byte[]{9}, new byte[0]));

        assertEquals(10, store.getSnapshotIndex(), "boundary must not move backwards");
        assertEquals(5, store.getSnapshotTerm());
        assertArrayEquals(new byte[]{1}, store.getSnapshot().stateMachineData,
                "the stale payload must not overwrite the newer one");
    }

    @Test
    void aSnapshotSurvivesAReopen() {
        byte[] smData = "state".getBytes();
        byte[] cfgData = "cfg".getBytes();
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(2, 1, smData, cfgData));
        reopen();

        assertEquals(2, store.getSnapshotIndex());
        assertEquals(1, store.getSnapshotTerm());
        assertEquals(2, store.getLastLogIndex());
        assertEquals(1, store.getTermAt(2), "the boundary term must still be known after recovery");
        assertArrayEquals(smData, store.getSnapshot().stateMachineData);
        assertArrayEquals(cfgData, store.getSnapshot().configurationData);
    }

    @Test
    void appendingPastASnapshotSurvivesAReopen() {
        store.appendEntries(List.of(entry(1, 1, "a"), entry(2, 1, "b")));
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(1, 1, new byte[0], new byte[0]));
        store.appendEntries(List.of(entry(3, 2, "c")));
        reopen();

        assertEquals(3, store.getLastLogIndex());
        assertEquals(2, store.getLastLogTerm());
        assertNull(store.getLogEntry(1), "compacted away");
        assertNotNull(store.getLogEntry(2));
        assertNotNull(store.getLogEntry(3));
    }

    // ---- helpers ---------------------------------------------------------

    protected static LogEntry entry(long index, long term, String command) {
        return LogEntry.newBuilder()
                .setIndex(index)
                .setTerm(term)
                .setCommand(ByteString.copyFromUtf8(command))
                .build();
    }
}
