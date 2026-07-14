/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Moves a Raft log from one {@link RaftStorage} implementation to another.
 *
 * <h2>Why this has to exist before the default can change</h2>
 * With snapshots disabled — which is how kwatro runs — <b>the log IS the persistence</b>. Swapping
 * the storage backend is therefore not a configuration change; it is a data migration. A node
 * restarted onto an empty WAL directory comes up with an empty log and an empty state machine,
 * while the cluster cheerfully reports {@code UP} because the remaining voters still form a
 * majority.
 *
 * <p>That is not a hypothetical: it is exactly the picture from July, when three of five nodes ran
 * for hours with an empty state machine and everything looked healthy. The difference is that this
 * time there would be no Berkeley DB log left to heal from.
 *
 * <h2>Why it is only ~40 lines</h2>
 * Because {@link RaftStorage} is a real interface and — since the storage contract suite — both
 * implementations demonstrably satisfy the same invariants. Copying between them is just reading
 * one and writing the other. That is the contract suite paying for itself.
 *
 * <h2>In place, and reversible</h2>
 * Berkeley DB writes {@code NNNNNNNN.jdb}; the WAL writes {@code wal-NNNNNN.log}. They do not
 * collide, so the migration runs inside the existing data directory and <b>leaves the Berkeley DB
 * files where they are</b>. Flip {@code storage.type} back and the old log is still there.
 *
 * <p>The escape hatch has a shelf life: once the node has been running on the WAL, the Berkeley DB
 * files are frozen at the moment of migration, and reverting loses everything written since. It is
 * a safety net for "this went wrong immediately", not a general undo.
 */
public final class StorageMigration {

    private static final Logger log = LoggerFactory.getLogger(StorageMigration.class);

    /** Entries copied per append — one fsync per batch rather than per entry. */
    private static final int BATCH = 500;

    private StorageMigration() { }

    /**
     * Copies a Berkeley DB log in {@code dataDir} into a WAL in the same directory, if — and only
     * if — there is a Berkeley DB log there and no WAL yet.
     *
     * <p>Idempotent by construction: once the WAL exists, this does nothing. So it is safe to call
     * on every startup, which is the point — a migration that has to be remembered for each of five
     * nodes is a migration that will be forgotten for one of them.
     *
     * @return {@code true} if a migration actually ran
     */
    public static boolean migrateBerkeleyDbToWalIfNeeded(Path dataDir) {
        if (!hasBerkeleyDbFiles(dataDir)) {
            return false;   // nothing to migrate from — a fresh node, or already WAL-native
        }
        if (hasWalFiles(dataDir)) {
            return false;   // already migrated; the .jdb files are the fallback copy
        }

        log.info("Berkeley-DB-Log gefunden, aber kein WAL — migriere {} nach WalStorage", dataDir);
        long entries;
        try (RaftStorage source = new BerkeleyDbStorage(dataDir.toFile());
             RaftStorage target = new WalStorage(dataDir.toFile())) {
            entries = copy(source, target);
        }
        log.info("Migration abgeschlossen: {} Einträge übernommen, lastLogIndex={}. "
                + "Die Berkeley-DB-Dateien bleiben als Rückfallebene liegen.", entries, entries);
        return true;
    }

    /**
     * Copies everything Figure 2 says must survive a restart — the log, {@code currentTerm},
     * {@code votedFor} and the snapshot — from {@code source} into {@code target}.
     *
     * @return the number of log entries copied
     * @throws IllegalStateException if {@code target} is not empty (this is a migration, not a
     *         merge: writing one log on top of another would produce a log that never existed)
     */
    public static long copy(RaftStorage source, RaftStorage target) {
        if (target.getLastLogIndex() != 0 || target.getSnapshotIndex() != 0) {
            throw new IllegalStateException(
                    "target storage is not empty (lastLogIndex=" + target.getLastLogIndex()
                            + ", snapshotIndex=" + target.getSnapshotIndex() + ")");
        }

        // The snapshot first: it sets the boundary the remaining entries sit above.
        RaftStorage.Snapshot snapshot = source.getSnapshot();
        if (snapshot != null) {
            target.saveSnapshotAndCompact(snapshot);
        }

        long from = source.getSnapshotIndex() + 1;
        long to = source.getLastLogIndex();
        long copied = 0;

        List<LogEntry> batch = new ArrayList<>(BATCH);
        for (long i = from; i <= to; i++) {
            LogEntry entry = source.getLogEntry(i);
            if (entry == null) {
                // A hole in the source log means the source is already broken. Better to stop
                // loudly here than to write a shorter log and let a node come up "fine".
                throw new IllegalStateException("gap in the source log at index " + i
                        + " (snapshotIndex=" + source.getSnapshotIndex()
                        + ", lastLogIndex=" + to + ")");
            }
            batch.add(entry);
            if (batch.size() == BATCH) {
                target.appendEntries(batch);
                copied += batch.size();
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            target.appendEntries(batch);
            copied += batch.size();
        }

        // §5.2: forgetting the vote lets a server vote twice in the same term. It is one line, and
        // it is the line a hand-rolled migration forgets.
        target.setTermAndVote(source.getCurrentTerm(), source.getVotedFor());

        return copied;
    }

    private static boolean hasBerkeleyDbFiles(Path dataDir) {
        return listMatching(dataDir, name -> name.endsWith(".jdb"));
    }

    private static boolean hasWalFiles(Path dataDir) {
        return listMatching(dataDir, name -> name.startsWith("wal-") && name.endsWith(".log"));
    }

    private static boolean listMatching(Path dataDir, java.util.function.Predicate<String> match) {
        File dir = dataDir.toFile();
        if (!dir.isDirectory()) return false;
        String[] names = dir.list();
        if (names == null) return false;
        for (String name : names) {
            if (match.test(name)) return true;
        }
        return false;
    }
}
