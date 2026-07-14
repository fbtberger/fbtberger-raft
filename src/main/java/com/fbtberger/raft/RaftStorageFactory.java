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
 * changed by editing two source files in two repositories is not really a choice. Now it is one
 * property, and it can be changed back.
 *
 * <h2>The default is {@link StorageType#WAL}</h2>
 * With one condition attached, and it is not optional: selecting the WAL runs
 * {@link StorageMigration#migrateBerkeleyDbToWalIfNeeded} first. With snapshots off — how kwatro
 * runs — <b>the log is the persistence</b>, so a node that starts on an empty WAL next to a
 * perfectly good Berkeley DB log comes up with an empty state machine and reports itself healthy.
 * Silently switching a backend would be a self-inflicted repeat of July.
 */
public final class RaftStorageFactory {

    private RaftStorageFactory() { }

    public enum StorageType {
        /** Segmented write-ahead log. The default. */
        WAL,
        /** Berkeley DB JE. What every node ran before this. */
        BDB,
        /** No disk at all — tests and demos only; a cluster on this loses everything on restart. */
        MEMORY;

        public static StorageType parse(String raw) {
            if (raw == null || raw.isBlank()) return WAL;
            try {
                return valueOf(raw.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("unknown storage type '" + raw
                        + "'; expected one of: wal, bdb, memory", e);
            }
        }
    }

    /** Opens the storage for {@code dataDir}, migrating an existing Berkeley DB log if we now want a WAL. */
    public static RaftStorage open(StorageType type, Path dataDir) {
        File dir = dataDir.toFile();
        //noinspection ResultOfMethodCallIgnored
        dir.mkdirs();

        return switch (type) {
            case MEMORY -> new InMemoryStorage();
            case BDB    -> new BerkeleyDbStorage(dir);
            case WAL    -> {
                // Idempotent, and deliberately automatic: a migration that each of five nodes has
                // to be told to run is a migration that one of them will not run.
                StorageMigration.migrateBerkeleyDbToWalIfNeeded(dataDir);
                yield new WalStorage(dir);
            }
        };
    }

    /** Convenience for Spring properties and {@code .properties} files. */
    public static RaftStorage open(String type, Path dataDir) {
        return open(StorageType.parse(type), dataDir);
    }
}
