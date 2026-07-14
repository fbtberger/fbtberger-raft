/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

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
 * Moving the log from Berkeley DB to the WAL.
 *
 * <p>This is the test that decides whether changing the default backend is a config change or a
 * data-loss incident. With snapshots off — how kwatro runs — <b>the log IS the persistence</b>. A
 * node restarted onto an empty WAL comes up with an empty state machine and reports {@code UP},
 * because the surviving voters still form a majority. That is July, self-inflicted.
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
            assertEquals(1, target.getLastLogTerm());
            assertEquals("cmd-2", target.getLogEntry(2).getCommand().toStringUtf8());
            // §5.2: a server that forgets its vote can vote twice in the same term. One line, and
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
            assertEquals(2, target.getSnapshotTerm());
            assertArrayEquals("state".getBytes(), target.getSnapshot().stateMachineData);
            assertArrayEquals("cfg".getBytes(), target.getSnapshot().configurationData);

            assertNull(target.getLogEntry(3), "compacted away in the source, and not resurrected here");
            assertNotNull(target.getLogEntry(4), "the entries above the boundary must come across");
            assertEquals(5, target.getLastLogIndex());
        }
    }

    /**
     * A migration is not a merge. Writing one log on top of another would produce a log that never
     * existed on any server — the one thing a replicated log may never be.
     */
    @Test
    void aTargetThatAlreadyHasALogIsRefused() {
        try (RaftStorage source = new BerkeleyDbStorage(new File(dataDir.toFile(), "src"));
             RaftStorage target = new WalStorage(new File(dataDir.toFile(), "dst"))) {

            source.appendEntries(entries(1, 2, 1));
            target.appendEntries(entries(1, 1, 9));   // not empty

            assertThrows(IllegalStateException.class, () -> StorageMigration.copy(source, target));
        }
    }

    // ── the automatic, in-place migration ────────────────────────────────────

    @Test
    void anExistingBerkeleyDbLogIsMigratedInPlace_andTheDbFilesAreLeftAsAFallback() {
        try (RaftStorage bdb = new BerkeleyDbStorage(dataDir.toFile())) {
            bdb.appendEntries(entries(1, 4, 3));
            bdb.setTermAndVote(5, "kwatro1");
        }

        assertTrue(StorageMigration.migrateBerkeleyDbToWalIfNeeded(dataDir));

        try (RaftStorage wal = new WalStorage(dataDir.toFile())) {
            assertEquals(4, wal.getLastLogIndex());
            assertEquals(3, wal.getLastLogTerm());
            assertEquals(5, wal.getCurrentTerm());
            assertEquals("kwatro1", wal.getVotedFor());
        }

        // The .jdb files are still there: flip storage.type back and the old log is intact.
        assertTrue(hasFile(name -> name.endsWith(".jdb")),
                "the Berkeley DB files must survive as the escape hatch");
    }

    /**
     * Idempotent, because it runs on every startup. A migration that has to be remembered for each
     * of five nodes is a migration that will be forgotten for one of them.
     */
    @Test
    void migratingTwiceDoesNothingTheSecondTime() {
        try (RaftStorage bdb = new BerkeleyDbStorage(dataDir.toFile())) {
            bdb.appendEntries(entries(1, 2, 1));
        }

        assertTrue(StorageMigration.migrateBerkeleyDbToWalIfNeeded(dataDir));
        assertFalse(StorageMigration.migrateBerkeleyDbToWalIfNeeded(dataDir),
                "the WAL already exists — a second run must not touch it");

        try (RaftStorage wal = new WalStorage(dataDir.toFile())) {
            assertEquals(2, wal.getLastLogIndex(), "and certainly must not duplicate the log");
        }
    }

    @Test
    void aFreshNodeHasNothingToMigrate() {
        assertFalse(StorageMigration.migrateBerkeleyDbToWalIfNeeded(dataDir));
    }

    // ── the factory ──────────────────────────────────────────────────────────

    @Test
    void theDefaultIsTheWal() {
        assertEquals(RaftStorageFactory.StorageType.WAL, RaftStorageFactory.StorageType.parse(null));
        assertEquals(RaftStorageFactory.StorageType.WAL, RaftStorageFactory.StorageType.parse(""));
        assertEquals(RaftStorageFactory.StorageType.WAL, RaftStorageFactory.StorageType.parse("wal"));
        assertEquals(RaftStorageFactory.StorageType.BDB, RaftStorageFactory.StorageType.parse("BDB"));
        assertEquals(RaftStorageFactory.StorageType.MEMORY, RaftStorageFactory.StorageType.parse(" memory "));
    }

    @Test
    void anUnknownStorageTypeIsRejectedLoudly() {
        // Silently falling back to a default here would be the worst possible behaviour: a typo in
        // one node's config would put it on a different backend than its peers, and nothing would
        // say so.
        assertThrows(IllegalArgumentException.class,
                () -> RaftStorageFactory.StorageType.parse("berkleydb"));
    }

    @Test
    void openingTheWalOnADirectoryWithABerkeleyDbLogMigratesIt() {
        try (RaftStorage bdb = new BerkeleyDbStorage(dataDir.toFile())) {
            bdb.appendEntries(entries(1, 3, 1));
        }

        try (RaftStorage opened = RaftStorageFactory.open("wal", dataDir)) {
            assertEquals(3, opened.getLastLogIndex(),
                    "the factory must not hand back an empty log next to a full Berkeley DB one");
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
