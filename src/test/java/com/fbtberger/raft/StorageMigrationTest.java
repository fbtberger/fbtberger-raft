/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.RaftStorageFactory.StorageType;
import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Moving the log between storage backends.
 *
 * <p>This decides whether changing the backend is a config change or a data-loss incident. The
 * on-disk log plus the latest snapshot ARE the persistence. A node that starts on an
 * empty or stale log comes up with the wrong state machine and reports {@code UP}, because the
 * surviving voters still form a majority. That is July, self-inflicted.
 */
class StorageMigrationTest {

    @TempDir
    Path dataDir;

    // ── the copy itself ──────────────────────────────────────────────────────

    @Test
    void everythingFigure2SaysMustSurviveARestartIsCopied() {
        try (RaftStorage bdb = new BerkeleyDbStorage(new File(dataDir.toFile(), "src"))) {
            bdb.appendEntries(entries(1, 3, 1));
            bdb.setTermAndVote(7, "kwatro2");
        }

        try (RaftStorage source = new BerkeleyDbStorage(new File(dataDir.toFile(), "src"));
             RaftStorage target = new WalStorage(new File(dataDir.toFile(), "dst"))) {

            assertEquals(3, StorageMigration.copy(source, target));

            assertEquals(3, target.getLastLogIndex());
            assertEquals("cmd-2", target.getLogEntry(2).getCommand().toStringUtf8());
            // §5.2: a server that forgets its vote can vote twice in the same term. One line — and
            // exactly the line a hand-rolled migration leaves out.
            assertEquals(7, target.getCurrentTerm());
            assertEquals("kwatro2", target.getVotedFor());
        }
    }

    @Test
    void aSnapshotAndTheEntriesAboveItAreBothCarriedOver() {
        try (RaftStorage bdb = new BerkeleyDbStorage(new File(dataDir.toFile(), "src"))) {
            bdb.appendEntries(entries(1, 5, 2));
            bdb.saveSnapshotAndCompact(
                    new RaftStorage.Snapshot(3, 2, "state".getBytes(), "cfg".getBytes()));
        }

        try (RaftStorage source = new BerkeleyDbStorage(new File(dataDir.toFile(), "src"));
             RaftStorage target = new WalStorage(new File(dataDir.toFile(), "dst"))) {

            StorageMigration.copy(source, target);

            assertEquals(3, target.getSnapshotIndex());
            assertArrayEquals("state".getBytes(), target.getSnapshot().stateMachineData);
            assertNull(target.getLogEntry(3), "compacted in the source, not resurrected here");
            assertNotNull(target.getLogEntry(4), "the entries above the boundary must come across");
            assertEquals(5, target.getLastLogIndex());
        }
    }

    /** A migration is not a merge: one log written over another existed on no server. */
    @Test
    void aTargetThatAlreadyHasALogIsRefused() {
        try (RaftStorage source = new BerkeleyDbStorage(new File(dataDir.toFile(), "src"));
             RaftStorage target = new WalStorage(new File(dataDir.toFile(), "dst"))) {

            source.appendEntries(entries(1, 2, 1));
            target.appendEntries(entries(1, 1, 9));

            assertThrows(IllegalStateException.class, () -> StorageMigration.copy(source, target));
        }
    }

    // ── reconcile: whichever log is further ahead wins ───────────────────────

    @Test
    void openingTheWalWhenOnlyABerkeleyDbLogExistsMigratesIt() {
        try (RaftStorage bdb = new BerkeleyDbStorage(dataDir.toFile())) {
            bdb.appendEntries(entries(1, 4, 3));
            bdb.setTermAndVote(5, "kwatro1");
        }

        assertTrue(StorageMigration.reconcile(StorageType.WAL, dataDir));

        try (RaftStorage wal = new WalStorage(dataDir.toFile())) {
            assertEquals(4, wal.getLastLogIndex());
            assertEquals(5, wal.getCurrentTerm());
            assertEquals("kwatro1", wal.getVotedFor());
        }
        assertTrue(hasFile(n -> n.endsWith(".jdb")), "the source log stays as the fallback");
    }

    /**
     * THE TRAP v113 SET, and the reason for v115.
     *
     * <p>Once a node has run on the WAL, the {@code .jdb} files are frozen at the moment of the
     * first migration. A naive "switch back to bdb" would load that stale log — <b>on all five
     * nodes at once</b>. No majority would hold the current state, so Raft would have nothing to
     * heal from, and everything written since would be gone. The cluster would look perfectly
     * healthy throughout.
     */
    @Test
    void switchingBackDoesNotResurrectTheStaleLog() {
        // A node that migrated to the WAL long ago...
        try (RaftStorage bdb = new BerkeleyDbStorage(dataDir.toFile())) {
            bdb.appendEntries(entries(1, 4, 1));            // the frozen .jdb log
        }
        StorageMigration.reconcile(StorageType.WAL, dataDir);

        // ...and has been running on it since.
        try (RaftStorage wal = new WalStorage(dataDir.toFile())) {
            wal.appendEntries(entries(5, 20, 2));           // everything since the migration
            wal.setTermAndVote(9, "kwatro3");
        }

        assertTrue(StorageMigration.reconcile(StorageType.BDB, dataDir),
                "the WAL is ahead — Berkeley DB must be rebuilt from it, not loaded as it stands");

        try (RaftStorage bdb = new BerkeleyDbStorage(dataDir.toFile())) {
            assertEquals(20, bdb.getLastLogIndex(), "the 16 entries written on the WAL must survive");
            assertEquals(2, bdb.getLastLogTerm());
            assertEquals(9, bdb.getCurrentTerm());
            assertEquals("kwatro3", bdb.getVotedFor());
        }
    }

    @Test
    void theSupersededLogIsMovedAsideAndNotDeleted() {
        try (RaftStorage bdb = new BerkeleyDbStorage(dataDir.toFile())) {
            bdb.appendEntries(entries(1, 4, 1));
        }
        StorageMigration.reconcile(StorageType.WAL, dataDir);
        try (RaftStorage wal = new WalStorage(dataDir.toFile())) {
            wal.appendEntries(entries(5, 10, 2));
        }

        StorageMigration.reconcile(StorageType.BDB, dataDir);

        // Deleting the stale log would destroy the only other copy if this migration is itself the
        // mistake. It gets moved, not removed.
        File[] archives = dataDir.toFile().listFiles(
                (dir, name) -> name.startsWith("superseded-bdb-"));
        assertNotNull(archives);
        assertEquals(1, archives.length, "the stale Berkeley DB files must be archived");
        assertTrue(archives[0].isDirectory());
    }

    /** Idempotent, because it runs on every boot. It must not copy back and forth for ever. */
    @Test
    void reconcilingTwiceDoesNothingTheSecondTime() {
        try (RaftStorage bdb = new BerkeleyDbStorage(dataDir.toFile())) {
            bdb.appendEntries(entries(1, 2, 1));
        }

        assertTrue(StorageMigration.reconcile(StorageType.WAL, dataDir));
        assertFalse(StorageMigration.reconcile(StorageType.WAL, dataDir),
                "the WAL now holds the same log — a second run must not touch it");

        try (RaftStorage wal = new WalStorage(dataDir.toFile())) {
            assertEquals(2, wal.getLastLogIndex(), "and certainly must not duplicate the log");
        }
    }

    @Test
    void aHigherTermWinsEvenWithAShorterLog() {
        // §5.4.1: up-to-dateness is (lastTerm, lastIndex), not length. A short log from a later
        // term beats a long one from an earlier term.
        try (RaftStorage wal = new WalStorage(dataDir.toFile())) {
            wal.appendEntries(entries(1, 10, 1));          // long, old
        }
        try (RaftStorage bdb = new BerkeleyDbStorage(dataDir.toFile())) {
            bdb.appendEntries(entries(1, 3, 5));           // short, new
        }

        assertTrue(StorageMigration.reconcile(StorageType.WAL, dataDir));

        try (RaftStorage wal = new WalStorage(dataDir.toFile())) {
            assertEquals(3, wal.getLastLogIndex());
            assertEquals(5, wal.getLastLogTerm());
        }
    }

    @Test
    void aFreshNodeHasNothingToReconcile() {
        assertFalse(StorageMigration.reconcile(StorageType.BDB, dataDir));
        assertFalse(StorageMigration.reconcile(StorageType.WAL, dataDir));
    }

    // ── the factory ──────────────────────────────────────────────────────────

    @Test
    void theDefaultIsBerkeleyDb() {
        // v113 defaulted to WAL on intuition; the benchmarks put it back. BDB recovers 7.8x faster
        // at 50k entries and writes 2.2x faster.
        assertEquals(StorageType.BDB, StorageType.parse(null));
        assertEquals(StorageType.BDB, StorageType.parse(""));
        assertEquals(StorageType.WAL, StorageType.parse("wal"));
        assertEquals(StorageType.BDB, StorageType.parse("BDB"));
        assertEquals(StorageType.MEMORY, StorageType.parse(" memory "));
    }

    @Test
    void anUnknownStorageTypeIsRejectedLoudly() {
        // Falling back to a default silently would be the worst behaviour available: a typo in one
        // node's config would put it on a different backend from its peers, and nothing would say so.
        assertThrows(IllegalArgumentException.class, () -> StorageType.parse("berkleydb"));
    }

    @Test
    void theFactoryNeverHandsBackAnEmptyLogNextToAFullOne() {
        try (RaftStorage wal = new WalStorage(dataDir.toFile())) {
            wal.appendEntries(entries(1, 3, 1));
        }

        try (RaftStorage opened = RaftStorageFactory.open("bdb", dataDir)) {
            assertEquals(3, opened.getLastLogIndex());
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private boolean hasFile(java.util.function.Predicate<String> match) {
        String[] names = dataDir.toFile().list();
        if (names == null) return false;
        for (String name : names) {
            if (match.test(name)) return true;
        }
        return false;
    }

    private static List<LogEntry> entries(int fromInclusive, int toInclusive, long term) {
        List<LogEntry> list = new ArrayList<>();
        for (int i = fromInclusive; i <= toInclusive; i++) {
            list.add(LogEntry.newBuilder()
                    .setIndex(i)
                    .setTerm(term)
                    .setCommand(ByteString.copyFromUtf8("cmd-" + i))
                    .build());
        }
        return list;
    }
}
