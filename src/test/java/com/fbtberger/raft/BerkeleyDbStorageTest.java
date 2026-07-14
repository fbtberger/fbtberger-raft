package com.fbtberger.raft;

import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The tests {@link BerkeleyDbStorage} never had.
 *
 * <p>{@code InMemoryStorageTest} claims in its own javadoc that "every invariant here applies
 * equally to BerkeleyDbStorage (same interface, same expected behaviour) -- these tests just run
 * without needing a real Berkeley DB environment on disk". That assumption is exactly what broke:
 * the durable implementation has behaviour the in-memory one cannot have — transactions and
 * cursors — and {@link BerkeleyDbStorage#truncateFrom(long)} committed its transaction while a
 * cursor was still open. Berkeley DB rejects that ("commit failed because there were open
 * cursors"), so <b>every truncate failed, always</b>.
 *
 * <p>It stayed invisible because only a follower that must catch up ever calls
 * {@code truncateFrom} (AppendEntries rule 3). On a cluster that never loses a node, the path is
 * never taken. On dev it took out three of five nodes for hours: each rejected the leader's log
 * with an unhandled exception, the leader reset {@code nextIndex} to 1 and resent everything, and
 * round it went. The cluster still looked healthy — the surviving two nodes were a majority.
 *
 * <p>A test against the real storage is therefore not a nicety. These run on a temp directory.
 */
class BerkeleyDbStorageTest {

    @TempDir
    Path tempDir;

    private BerkeleyDbStorage store;

    private static LogEntry entry(long index, long term) {
        return LogEntry.newBuilder()
                .setIndex(index)
                .setTerm(term)
                .setCommand(ByteString.copyFromUtf8("cmd-" + index))
                .build();
    }

    @BeforeEach
    void setUp() {
        store = new BerkeleyDbStorage(new File(tempDir.toFile(), "raft"));
    }

    @AfterEach
    void tearDown() {
        if (store != null) store.close();
    }

    @Test
    void truncateFrom_deletesTheEntryAndEverythingAfterIt() {
        // THE regression test: before the fix this threw
        // IllegalStateException("commit failed because there were open cursors").
        store.appendEntries(List.of(entry(1, 1), entry(2, 1), entry(3, 2), entry(4, 2), entry(5, 2)));

        store.truncateFrom(3);

        assertEquals(2, store.getLastLogIndex());
        assertEquals(1, store.getLastLogTerm());
        assertNotNull(store.getLogEntry(2));
        assertNull(store.getLogEntry(3));
        assertNull(store.getLogEntry(5));
    }

    @Test
    void truncateFrom_thenAppend_isHowAFollowerAdoptsTheLeadersLog() {
        // AppendEntries rule 3 + 4: discard the conflict, then take the leader's version. This is
        // the exact sequence a lagging follower runs — the one that was failing in production.
        store.appendEntries(List.of(entry(1, 1), entry(2, 1), entry(3, 1)));

        store.truncateFrom(2);
        store.appendEntries(List.of(entry(2, 5), entry(3, 5), entry(4, 5)));

        assertEquals(4, store.getLastLogIndex());
        assertEquals(5, store.getLastLogTerm());
        assertEquals(5, store.getLogEntry(2).getTerm());   // the leader's version won
        assertEquals(5, store.getTermAt(3));
    }

    @Test
    void truncateFrom_isIdempotent_andToleratesAnIndexBeyondTheLog() {
        store.appendEntries(List.of(entry(1, 1), entry(2, 1)));

        store.truncateFrom(5);        // nothing there — must not throw
        assertEquals(2, store.getLastLogIndex());

        store.truncateFrom(1);        // wipe the log
        assertEquals(0, store.getLastLogIndex());

        store.truncateFrom(1);        // again, on an empty log
        assertEquals(0, store.getLastLogIndex());
    }

    @Test
    void theLogSurvivesAReopen() {
        store.appendEntries(List.of(entry(1, 1), entry(2, 3)));
        store.setTermAndVote(7, "kwatro2");
        store.close();

        store = new BerkeleyDbStorage(new File(tempDir.toFile(), "raft"));

        assertEquals(2, store.getLastLogIndex());
        assertEquals(3, store.getLastLogTerm());
        assertEquals(7, store.getCurrentTerm());
        assertEquals("kwatro2", store.getVotedFor());
    }

    @Test
    void aTruncateSurvivesAReopen() {
        store.appendEntries(List.of(entry(1, 1), entry(2, 1), entry(3, 1)));
        store.truncateFrom(2);
        store.close();

        store = new BerkeleyDbStorage(new File(tempDir.toFile(), "raft"));

        assertEquals(1, store.getLastLogIndex());
        assertNull(store.getLogEntry(2));   // the deletion was really committed, not just cached
    }

    @Test
    void snapshotAndCompact_discardsCoveredEntries_butKeepsTheBoundaryTerm() {
        store.appendEntries(List.of(entry(1, 1), entry(2, 1), entry(3, 2), entry(4, 2)));

        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(
                2, 1, "state".getBytes(), "config".getBytes()));

        assertEquals(2, store.getSnapshotIndex());
        assertEquals(1, store.getSnapshotTerm());
        assertNull(store.getLogEntry(1));       // compacted away
        assertNotNull(store.getLogEntry(3));    // still there
        assertEquals(1, store.getTermAt(2));    // term at the boundary is still known (§7)
        assertEquals(4, store.getLastLogIndex());
    }
}
