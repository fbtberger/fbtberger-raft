/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.RaftStorageFactory.StorageType;
import com.fbtberger.raft.proto.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;

/**
 * Keeps the on-disk log correct when the storage backend changes.
 *
 * <h2>Why this is not just "copy A to B"</h2>
 * <b>The on-disk log plus the latest snapshot ARE the persistence</b> (snapshots are enabled;
 * kwatro runs with a threshold of 1000 on dev / 300 in prod). Switching backends
 * is a data migration, and a node that starts on an empty or stale log comes up with a wrong state
 * machine while the cluster reports {@code UP}, because the remaining voters still form a majority.
 * That is the July picture, self-inflicted.
 *
 * <h2>The rule (v115): whichever log is further ahead wins</h2>
 * The first version only asked "does a WAL exist yet?". That is enough exactly once — on the way
 * OUT of Berkeley DB. It sets a trap for the way back: after the node has run on the WAL, the
 * {@code .jdb} files are frozen at the moment of that first migration, and a naive switch back to
 * {@code bdb} would load that stale log <b>on all five nodes at once</b>. No majority would hold the
 * current state, so Raft would have nothing to heal from. Everything since the migration would be
 * gone — and the cluster would look perfectly healthy while it happened.
 *
 * <p>{@link #reconcile} therefore does not care which direction it is going. It compares what is
 * actually on disk, using Raft's own "more up to date" test (§5.4.1: a higher last term wins; on
 * equal terms, the longer log), and copies the winner into the backend about to be opened. The
 * loser's files are <b>moved aside, never deleted</b> — into {@code superseded-<type>-<timestamp>/},
 * where a human can still find them.
 *
 * <p>Idempotent, so it runs on every startup. A migration that must be remembered separately for
 * each of five nodes is a migration that will be forgotten for one of them.
 */
public final class StorageMigration {

    private static final Logger log = LoggerFactory.getLogger(StorageMigration.class);

    /** Entries copied per append — one fsync per batch rather than one per entry. */
    private static final int BATCH = 500;

    private StorageMigration() { }

    /**
     * Makes {@code target} in {@code dataDir} hold the most recent log available there, migrating
     * from the other backend if that one is further ahead.
     *
     * @return {@code true} if a migration actually ran
     */
    public static boolean reconcile(StorageType target, Path dataDir) {
        if (target == StorageType.MEMORY) return false;

        StorageType other = target == StorageType.WAL ? StorageType.BDB : StorageType.WAL;
        if (!hasFilesFor(other, dataDir)) {
            return false;   // nothing to migrate from
        }

        LogPosition targetPos = positionOf(target, dataDir);
        LogPosition otherPos = positionOf(other, dataDir);

        if (!otherPos.isMoreUpToDateThan(targetPos)) {
            // The backend we are opening already holds the current log. This is also what makes a
            // second boot a no-op instead of copying back and forth for ever.
            return false;
        }

        log.warn("{}-Log ist weiter als {} ({} vs {}) — migriere {} -> {}",
                other, target, otherPos, targetPos, other, target);

        if (hasFilesFor(target, dataDir)) {
            // The target's files are STALE. Using them would silently roll this node back; deleting
            // them would destroy the only other copy if this migration is itself a mistake. Move.
            Path archive = archiveFilesFor(target, dataDir);
            log.warn("Veralteter {}-Log beiseitegeschoben nach {}", target, archive);
        }

        long copied;
        try (RaftStorage source = openRaw(other, dataDir);
             RaftStorage destination = openRaw(target, dataDir)) {
            copied = copy(source, destination);
        }
        log.info("Migration abgeschlossen: {} Einträge {} -> {}. Der {}-Log bleibt als "
                + "Rückfallebene liegen.", copied, other, target, other);
        return true;
    }

    /**
     * Copies everything Figure 2 says must survive a restart — the log, {@code currentTerm},
     * {@code votedFor} and the snapshot — from {@code source} into {@code target}.
     *
     * @return the number of log entries copied
     * @throws IllegalStateException if {@code target} is not empty. This is a migration, not a
     *         merge: writing one log on top of another would produce a log that existed on no
     *         server, which is the one thing a replicated log may never be.
     */
    public static long copy(RaftStorage source, RaftStorage target) {
        if (target.getLastLogIndex() != 0 || target.getSnapshotIndex() != 0) {
            throw new IllegalStateException(
                    "target storage is not empty (lastLogIndex=" + target.getLastLogIndex()
                            + ", snapshotIndex=" + target.getSnapshotIndex() + ")");
        }

        RaftStorage.Snapshot snapshot = source.getSnapshot();
        if (snapshot != null) {
            target.saveSnapshotAndCompact(snapshot);   // sets the boundary the entries sit above
        }

        long from = source.getSnapshotIndex() + 1;
        long to = source.getLastLogIndex();
        long copied = 0;

        List<LogEntry> batch = new ArrayList<>(BATCH);
        for (long i = from; i <= to; i++) {
            LogEntry entry = source.getLogEntry(i);
            if (entry == null) {
                // A hole in the source means the source is already broken. Stop loudly rather than
                // write a shorter log and let the node come up looking fine.
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

        // §5.2: a server that forgets its vote can vote twice in the same term. One line — and
        // exactly the line a hand-rolled migration leaves out.
        target.setTermAndVote(source.getCurrentTerm(), source.getVotedFor());

        return copied;
    }

    // ── which log is further ahead? ──────────────────────────────────────────

    /** Raft's own comparison (§5.4.1): a higher last term wins; on equal terms, the longer log. */
    record LogPosition(long lastTerm, long lastIndex) {

        static final LogPosition EMPTY = new LogPosition(0, 0);

        boolean isMoreUpToDateThan(LogPosition other) {
            if (lastIndex == 0) return false;   // an empty log is never ahead of anything
            if (lastTerm != other.lastTerm) return lastTerm > other.lastTerm;
            return lastIndex > other.lastIndex;
        }

        @Override
        public String toString() {
            return "term=" + lastTerm + ",index=" + lastIndex;
        }
    }

    static LogPosition positionOf(StorageType type, Path dataDir) {
        if (!hasFilesFor(type, dataDir)) return LogPosition.EMPTY;
        try (RaftStorage store = openRaw(type, dataDir)) {
            return new LogPosition(store.getLastLogTerm(), store.getLastLogIndex());
        }
    }

    /** Opens a backend WITHOUT reconciling — otherwise this would recurse. */
    private static RaftStorage openRaw(StorageType type, Path dataDir) {
        File dir = dataDir.toFile();
        return switch (type) {
            case WAL -> new WalStorage(dir);
            case BDB -> new BerkeleyDbStorage(dir);
            case MEMORY -> new InMemoryStorage();
        };
    }

    // ── files ────────────────────────────────────────────────────────────────

    private static Predicate<String> filePatternFor(StorageType type) {
        return switch (type) {
            case WAL -> name -> name.startsWith("wal-") && name.endsWith(".log");
            case BDB -> name -> name.endsWith(".jdb");
            case MEMORY -> name -> false;
        };
    }

    static boolean hasFilesFor(StorageType type, Path dataDir) {
        return !listFilesFor(type, dataDir).isEmpty();
    }

    private static List<Path> listFilesFor(StorageType type, Path dataDir) {
        File dir = dataDir.toFile();
        List<Path> found = new ArrayList<>();
        if (!dir.isDirectory()) return found;
        String[] names = dir.list();
        if (names == null) return found;
        Predicate<String> matches = filePatternFor(type);
        for (String name : names) {
            if (matches.test(name)) found.add(dataDir.resolve(name));
        }
        return found;
    }

    /** Moves a stale backend's files into {@code superseded-<type>-<timestamp>/}. Never deletes. */
    private static Path archiveFilesFor(StorageType type, Path dataDir) {
        String stamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        Path archive = dataDir.resolve(
                "superseded-" + type.name().toLowerCase(Locale.ROOT) + "-" + stamp);
        try {
            Files.createDirectories(archive);
            for (Path file : listFilesFor(type, dataDir)) {
                Files.move(file, archive.resolve(file.getFileName()),
                        StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "could not move the stale " + type + " log aside; refusing to continue", e);
        }
        return archive;
    }
}
