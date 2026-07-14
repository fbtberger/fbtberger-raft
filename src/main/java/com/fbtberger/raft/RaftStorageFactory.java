/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.io.File;
import java.nio.file.Path;
import java.util.Locale;

/**
 * Chooses the storage backend — which used to be a hard-coded {@code new BerkeleyDbStorage(...)} in
 * two different Spring configurations.
 *
 * <p>Making it configurable matters more than which one is the default: a choice that can only be
 * changed by editing two source files in two repositories is not really a choice.
 *
 * <h2>The default is {@link StorageType#BDB} — and this time there are numbers</h2>
 * v113 switched the default to the WAL on the assumption that appending to a log must beat a B-tree.
 * The JMH benchmarks (v112/v114), run on the machine the cluster actually runs on, say otherwise:
 *
 * <pre>
 *   recovery @ 50k entries   WAL 378 ms   BDB  49 ms    BDB 7.8x   (WAL is linear; BDB is not)
 *   append, blocking fsync   WAL 737 ms   BDB 337 ms    BDB 2.2x
 *   append, deferred fsync   WAL 356 ms   BDB 303 ms    BDB 1.2x
 * </pre>
 *
 * Berkeley DB wins on every axis measured. The WAL scans every entry to recover; Berkeley DB opens
 * a B-tree. So the default goes back, on evidence this time rather than on intuition — which is
 * what the benchmarks were built for, and they earned their keep by contradicting the person who
 * wrote them.
 *
 * <p>(The deferred-fsync machinery of §10.2.1 is vindicated either way: it halves the WAL's write
 * cost. It simply does less for Berkeley DB, which already groups its writes.)
 *
 * <h2>Changing the backend is a data migration</h2>
 * Selecting a backend runs {@link StorageMigration#reconcile} first, in EITHER direction. With
 * snapshots off the log IS the persistence, so a node that starts on a stale or empty log comes up
 * with the wrong state machine and reports itself healthy. Whichever log on disk is further ahead
 * wins; the other is moved aside, never deleted.
 */
public final class RaftStorageFactory {

    private RaftStorageFactory() { }

    public enum StorageType {
        /** Segmented write-ahead log. */
        WAL,
        /** Berkeley DB JE. The default — see the class javadoc for the numbers. */
        BDB,
        /** No disk at all — tests and demos only; a cluster on this loses everything on restart. */
        MEMORY;

        public static StorageType parse(String raw) {
            if (raw == null || raw.isBlank()) return BDB;
            try {
                return valueOf(raw.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("unknown storage type '" + raw
                        + "'; expected one of: wal, bdb, memory", e);
            }
        }
    }

    /** Opens the storage for {@code dataDir}, migrating from the other backend if its log is ahead. */
    public static RaftStorage open(StorageType type, Path dataDir) {
        File dir = dataDir.toFile();
        //noinspection ResultOfMethodCallIgnored
        dir.mkdirs();

        // Idempotent, and deliberately automatic: a migration that each of five nodes has to be
        // told to run separately is a migration one of them will not run. Symmetric, so switching
        // BACK is as safe as switching forward -- which the first version was not.
        StorageMigration.reconcile(type, dataDir);

        return switch (type) {
            case MEMORY -> new InMemoryStorage();
            case BDB    -> new BerkeleyDbStorage(dir);
            case WAL    -> new WalStorage(dir);
        };
    }

    /** Convenience for Spring properties and {@code .properties} files. */
    public static RaftStorage open(String type, Path dataDir) {
        return open(StorageType.parse(type), dataDir);
    }
}
