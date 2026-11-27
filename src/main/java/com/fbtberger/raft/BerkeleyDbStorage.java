/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@link RaftStorage} backed by Berkeley DB Java Edition -- the durable
 * implementation meant for actually running a cluster.
 *
 * Holds exactly the fields the paper marks as persistent in Figure 2 --
 * currentTerm, votedFor, and the log -- and fsyncs every write (via
 * Durability.COMMIT_SYNC) before returning, since the algorithm requires
 * this state to survive a crash before a server can safely respond to an
 * RPC. commitIndex, lastApplied, and the leader-only nextIndex/matchIndex
 * arrays are volatile per that same figure, so they live only in memory,
 * over in RaftNode.
 */
public final class BerkeleyDbStorage implements RaftStorage {

    private static final byte[] KEY_CURRENT_TERM = "currentTerm".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEY_VOTED_FOR = "votedFor".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEY_SNAPSHOT_INDEX = "snapshotIndex".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEY_SNAPSHOT_TERM = "snapshotTerm".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEY_SNAPSHOT_STATE = "snapshotState".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEY_SNAPSHOT_CONFIG = "snapshotConfig".getBytes(StandardCharsets.UTF_8);
    // Absent in an environment written before client sessions existed; readBytes turns
    // that into an empty table, which is exactly what such a snapshot holds.
    private static final byte[] KEY_SNAPSHOT_SESSIONS = "snapshotSessions".getBytes(StandardCharsets.UTF_8);
    // Name of the file under snapshots/ that holds the payload. Absent for a snapshot
    // stored before payloads moved out of the database: that one is still the value of
    // KEY_SNAPSHOT_STATE and is read from there until the next snapshot replaces it.
    private static final byte[] KEY_SNAPSHOT_FILE = "snapshotFile".getBytes(StandardCharsets.UTF_8);

    private final Environment env;
    private final Database metaDb; // currentTerm, votedFor, snapshot metadata + payload
    private final Database logDb;  // index -> LogEntry
    private final ExecutorService syncExecutor;

    // Cached in memory and recomputed from disk at startup, so it always
    // reflects what's actually durable.
    private volatile long lastLogIndex;
    private volatile long lastLogTerm;
    // Cached snapshot boundary (§7); the payload itself (state machine +
    // configuration bytes) is read from disk on demand via getSnapshot()
    // rather than cached here, since it can be arbitrarily large.
    private volatile long snapshotIndex;
    private volatile long snapshotTerm;

    /**
     * Where snapshot payloads live. Beside the database, not in it: a payload is one large
     * value that is written once and read sequentially, which is what a file is for, and
     * as a database value it could only ever be read and written as a single array.
     */
    private final File snapshotDir;
    /** The current payload file's name, or null while the payload is still a database value. */
    private volatile String snapshotFileName;
    private final java.util.concurrent.atomic.AtomicLong snapshotTmpCounter = new java.util.concurrent.atomic.AtomicLong();

    public BerkeleyDbStorage(File dataDir) {
        if (!dataDir.exists() && !dataDir.mkdirs()) {
            throw new IllegalStateException("could not create data dir: " + dataDir);
        }
        this.snapshotDir = new File(dataDir, "snapshots");
        if (!snapshotDir.exists() && !snapshotDir.mkdirs()) {
            throw new IllegalStateException("could not create snapshot dir: " + snapshotDir);
        }
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        envConfig.setDurability(Durability.COMMIT_SYNC);

        this.env = new Environment(dataDir, envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);

        this.metaDb = env.openDatabase(null, "raftMeta", dbConfig);
        this.logDb = env.openDatabase(null, "raftLog", dbConfig);
        this.syncExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "bdb-sync");
            t.setDaemon(true);
            return t;
        });

        recoverSnapshotBounds();
        recoverCachedLogBounds();
    }

    private void recoverSnapshotBounds() {
        this.snapshotIndex = readLong(KEY_SNAPSHOT_INDEX);
        this.snapshotTerm = readLong(KEY_SNAPSHOT_TERM);
        byte[] fileName = readBytes(KEY_SNAPSHOT_FILE);
        this.snapshotFileName = fileName.length == 0 ? null : new String(fileName, StandardCharsets.UTF_8);
        // Whatever else is in the directory was never committed: a payload whose
        // transaction did not happen, or a temporary file from a write that died.
        File[] files = snapshotDir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (!file.getName().equals(snapshotFileName)) file.delete();
            }
        }
    }

    private void recoverCachedLogBounds() {
        try (Cursor cursor = logDb.openCursor(null, null)) {
            DatabaseEntry key = new DatabaseEntry();
            DatabaseEntry value = new DatabaseEntry();
            if (cursor.getLast(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                LogEntry entry = parseEntry(value.getData());
                this.lastLogIndex = entry.getIndex();
                this.lastLogTerm = entry.getTerm();
            } else {
                // No entries physically in the log -- either nothing has
                // ever been appended, or everything has been compacted away
                // by a snapshot (§7), in which case that snapshot's boundary
                // is the right thing to report as "last log index/term".
                this.lastLogIndex = snapshotIndex;
                this.lastLogTerm = snapshotTerm;
            }
        }
    }

    private long readLong(byte[] key) {
        DatabaseEntry value = new DatabaseEntry();
        OperationStatus status = metaDb.get(null, new DatabaseEntry(key), value, LockMode.DEFAULT);
        return status == OperationStatus.SUCCESS ? bytesToLong(value.getData()) : 0L;
    }

    // ---- currentTerm / votedFor -------------------------------------

    @Override
    public synchronized long getCurrentTerm() {
        DatabaseEntry value = new DatabaseEntry();
        OperationStatus status = metaDb.get(null, new DatabaseEntry(KEY_CURRENT_TERM), value, LockMode.DEFAULT);
        if (status != OperationStatus.SUCCESS) return 0L;
        return bytesToLong(value.getData());
    }

    @Override
    public synchronized String getVotedFor() {
        DatabaseEntry value = new DatabaseEntry();
        OperationStatus status = metaDb.get(null, new DatabaseEntry(KEY_VOTED_FOR), value, LockMode.DEFAULT);
        if (status != OperationStatus.SUCCESS) return null;
        byte[] data = value.getData();
        return data.length == 0 ? null : new String(data, StandardCharsets.UTF_8);
    }

    /**
     * Persists a new currentTerm together with votedFor in one transaction.
     * Used whenever the two change together: starting an election bumps
     * the term and votes for self in the same step (§5.2); stepping down
     * because of a newer term clears votedFor for that new term.
     */
    @Override
    public synchronized void setTermAndVote(long term, String votedFor) {
        Transaction txn = env.beginTransaction(null, null);
        try {
            metaDb.put(txn, new DatabaseEntry(KEY_CURRENT_TERM), new DatabaseEntry(longToBytes(term)));
            byte[] voteBytes = votedFor == null ? new byte[0] : votedFor.getBytes(StandardCharsets.UTF_8);
            metaDb.put(txn, new DatabaseEntry(KEY_VOTED_FOR), new DatabaseEntry(voteBytes));
            txn.commit();
        } catch (RuntimeException e) {
            txn.abort();
            throw e;
        }
    }

    // ---- log ----------------------------------------------------------

    @Override
    public synchronized LogEntry getLogEntry(long index) {
        if (index <= 0) return null;
        DatabaseEntry value = new DatabaseEntry();
        OperationStatus status = logDb.get(null, new DatabaseEntry(longToBytes(index)), value, LockMode.DEFAULT);
        if (status != OperationStatus.SUCCESS) return null;
        return parseEntry(value.getData());
    }

    @Override
    public long getLastLogIndex() { return lastLogIndex; }

    @Override
    public long getLastLogTerm() { return lastLogTerm; }

    /**
     * Appends entries to the log. Callers are responsible for resolving any
     * conflicts (via truncateFrom) before calling this, so the entries
     * passed in always extend the log rather than overwrite it in place.
     */
    @Override
    public synchronized void appendEntries(Iterable<LogEntry> entries) {
        appendWithDurability(entries, null);
    }

    /**
     * §10.2.1: appends entries with {@link Durability#COMMIT_WRITE_NO_SYNC}
     * so they are immediately readable for replication, then syncs to disk
     * on a background thread. The returned future completes once the data
     * is durable.
     */
    @Override
    public synchronized CompletableFuture<Void> appendEntriesDeferSync(Iterable<LogEntry> entries) {
        appendWithDurability(entries,
                new com.sleepycat.je.TransactionConfig()
                        .setDurability(Durability.COMMIT_WRITE_NO_SYNC));
        return CompletableFuture.runAsync(() -> env.flushLog(true), syncExecutor);
    }

    private void appendWithDurability(Iterable<LogEntry> entries, com.sleepycat.je.TransactionConfig txnConfig) {
        Transaction txn = env.beginTransaction(null, txnConfig);
        try {
            long newLastIndex = lastLogIndex;
            long newLastTerm = lastLogTerm;
            for (LogEntry entry : entries) {
                logDb.put(txn, new DatabaseEntry(longToBytes(entry.getIndex())), new DatabaseEntry(entry.toByteArray()));
                newLastIndex = entry.getIndex();
                newLastTerm = entry.getTerm();
            }
            txn.commit();
            this.lastLogIndex = newLastIndex;
            this.lastLogTerm = newLastTerm;
        } catch (RuntimeException e) {
            txn.abort();
            throw e;
        }
    }

    /**
     * Deletes the entry at fromIndexInclusive and everything after it. This
     * is how a follower discards entries that conflict with the leader's
     * log (Figure 2, AppendEntries rule 3); leaders themselves never delete
     * from their own log (the Leader Append-Only property, Figure 3).
     * In correct operation this is never called at or below the current
     * snapshot boundary (§7) -- {@link RaftNode} skips any entry already
     * covered by the snapshot before it would ever reach here -- but the
     * cached bounds are floored at the snapshot regardless, defensively.
     */
    @Override
    public synchronized void truncateFrom(long fromIndexInclusive) {
        Transaction txn = env.beginTransaction(null, null);
        boolean committed = false;
        try {
            // The cursor MUST be closed before the transaction is committed. Committing while a
            // cursor is still open makes Berkeley DB throw
            // "Transaction N commit failed because there were open cursors" — which is exactly
            // what happened here: the commit sat INSIDE the try-with-resources, so the cursor was
            // still open. Every truncate therefore failed, forever.
            //
            // The blast radius was far worse than the typo suggests. truncateFrom() is only
            // reached by a follower that has to catch up (AppendEntries rule 3), so a node that
            // never fell behind never touched it — while a node that DID fall behind rejected the
            // leader's log with an unhandled exception, the leader reset nextIndex to 1, resent
            // the entire log, and the receiver threw again: an endless loop. Three of five nodes
            // sat with an empty state machine for hours; the cluster looked healthy because the
            // remaining two still formed a majority, and clients reading from the empty nodes were
            // told their games did not exist.
            try (Cursor cursor = logDb.openCursor(txn, null)) {
                DatabaseEntry key = new DatabaseEntry(longToBytes(fromIndexInclusive));
                DatabaseEntry value = new DatabaseEntry();
                OperationStatus status = cursor.getSearchKeyRange(key, value, LockMode.DEFAULT);
                while (status == OperationStatus.SUCCESS) {
                    cursor.delete();
                    status = cursor.getNext(key, value, LockMode.DEFAULT);
                }
            }   // ← cursor closed here, before the commit
            txn.commit();
            committed = true;
        } finally {
            if (!committed) txn.abort();
        }
        if (fromIndexInclusive <= lastLogIndex) {
            long newLastIndex = fromIndexInclusive - 1;
            if (newLastIndex <= snapshotIndex) {
                this.lastLogIndex = snapshotIndex;
                this.lastLogTerm = snapshotTerm;
            } else {
                LogEntry prev = getLogEntry(newLastIndex);
                this.lastLogIndex = newLastIndex;
                this.lastLogTerm = prev == null ? 0 : prev.getTerm();
            }
        }
    }

    // ---- snapshotting (§7) --------------------------------------------

    @Override
    public long getSnapshotIndex() { return snapshotIndex; }

    @Override
    public long getSnapshotTerm() { return snapshotTerm; }

    @Override
    public synchronized Snapshot getSnapshot() {
        if (snapshotIndex == 0) return null;
        byte[] stateData;
        if (snapshotFileName == null) {
            stateData = readBytes(KEY_SNAPSHOT_STATE);
        } else {
            try {
                stateData = java.nio.file.Files.readAllBytes(payloadFile().toPath());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        byte[] configData = readBytes(KEY_SNAPSHOT_CONFIG);
        return new Snapshot(snapshotIndex, snapshotTerm, stateData, configData, readBytes(KEY_SNAPSHOT_SESSIONS));
    }

    private File payloadFile() {
        return new File(snapshotDir, snapshotFileName);
    }

    @Override
    public synchronized SnapshotMeta getSnapshotMeta() {
        if (snapshotIndex == 0) return null;
        long length = snapshotFileName == null ? readBytes(KEY_SNAPSHOT_STATE).length : payloadFile().length();
        return new SnapshotMeta(snapshotIndex, snapshotTerm, readBytes(KEY_SNAPSHOT_CONFIG),
                readBytes(KEY_SNAPSHOT_SESSIONS), length);
    }

    @Override
    public synchronized java.io.InputStream openSnapshotData() throws IOException {
        if (snapshotIndex == 0) return null;
        if (snapshotFileName == null) {
            return new java.io.ByteArrayInputStream(readBytes(KEY_SNAPSHOT_STATE));
        }
        // Stays readable if a newer snapshot replaces this one meanwhile: deleting the old
        // file removes its name, not the data this descriptor is reading.
        return new java.io.BufferedInputStream(new java.io.FileInputStream(payloadFile()), 1 << 16);
    }

    @Override
    public synchronized int readSnapshotData(long lastIncludedIndex, long offset, byte[] target,
                                             int targetOffset, int length) throws IOException {
        if (snapshotIndex == 0 || snapshotIndex != lastIncludedIndex) {
            return -1;
        }
        if (snapshotFileName == null) {
            byte[] inline = readBytes(KEY_SNAPSHOT_STATE);
            int available = (int) Math.max(0, Math.min(length, inline.length - offset));
            System.arraycopy(inline, (int) offset, target, targetOffset, available);
            return available;
        }
        try (java.io.RandomAccessFile file = new java.io.RandomAccessFile(payloadFile(), "r")) {
            int available = (int) Math.max(0, Math.min(length, file.length() - offset));
            if (available == 0) return 0;
            file.seek(offset);
            file.readFully(target, targetOffset, available);
            return available;
        }
    }

    @Override
    public void saveSnapshotAndCompact(Snapshot snapshot) {
        try (SnapshotSink sink = beginSnapshot(snapshot.lastIncludedIndex, snapshot.lastIncludedTerm)) {
            sink.stream().write(snapshot.stateMachineData);
            sink.commit(snapshot.configurationData, snapshot.sessionData);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes the payload to a temporary file under {@code snapshots/}. At commit that file
     * is made durable and given its final name, and only then does the transaction name it
     * as the snapshot and delete the log prefix -- so the database never points at a file
     * that is not completely on disk. A crash between the two leaves an unreferenced file,
     * which the next start removes.
     */
    @Override
    public SnapshotSink beginSnapshot(long lastIncludedIndex, long lastIncludedTerm) throws IOException {
        File tmp = new File(snapshotDir, "snapshot-" + lastIncludedIndex + "-"
                + snapshotTmpCounter.incrementAndGet() + ".tmp");
        java.io.FileOutputStream file = new java.io.FileOutputStream(tmp);
        java.io.OutputStream payload = new java.io.BufferedOutputStream(file, 1 << 16);
        return new SnapshotSink() {
            private boolean finished;

            @Override public java.io.OutputStream stream() { return payload; }

            @Override
            public void commit(byte[] configurationData, byte[] sessionData) throws IOException {
                payload.flush();
                file.getFD().sync();
                file.close();
                finished = true;
                install(tmp, lastIncludedIndex, lastIncludedTerm, configurationData, sessionData);
            }

            @Override
            public void close() {
                if (finished) return;
                finished = true;
                try {
                    file.close();
                } catch (IOException ignored) {
                    // the file is being thrown away
                }
                tmp.delete();
            }
        };
    }

    /**
     * Makes the payload in {@code tmp} the stored snapshot, then -- in one
     * transaction, so metadata and log never disagree even across a crash -- records
     * it and deletes every log entry at or before {@code lastIncludedIndex}. Any
     * entries already in the log beyond that index are left exactly as they
     * were; only a prefix is ever discarded.
     */
    private synchronized void install(File tmp, long lastIncludedIndex, long lastIncludedTerm,
                                      byte[] configurationData, byte[] sessionData) throws IOException {
        // One transaction, and the monitor held throughout. Splitting the deletion into
        // batches with the monitor released between them was tried and measured on the Pi
        // cluster: at a threshold of 20000 the snapshot went from 450 ms to 760 ms -- forty
        // transactions instead of one, each with its own commit -- and the stall it was
        // meant to remove was still there, election and all. So appendEntries waiting on
        // this monitor is not what stops the cluster; the cost is inside the BDB
        // environment, where deleting twenty thousand records is journal traffic, cleaner
        // and checkpointer work that competes with appends whoever holds what.
        //
        // Which leaves the threshold as the lever: see the demo cluster's node.properties,
        // where it is bounded from both sides and says why.
        //
        // The snapshot boundary must never move backwards: a background (COW)
        // snapshot in RaftNode decides to save off-lock, so if a newer snapshot
        // (e.g. one just installed by InstallSnapshot after a step-down/
        // re-election) landed in between, that stale save is dropped here rather
        // than overwriting the higher boundary and rewinding compaction.
        if (snapshotIndex != 0 && lastIncludedIndex <= snapshotIndex) {
            tmp.delete();
            return;
        }
        // Durable and under its final name before the transaction refers to it.
        String fileName = "snapshot-" + lastIncludedIndex + ".data";
        java.nio.file.Files.move(tmp.toPath(), new File(snapshotDir, fileName).toPath(),
                java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        Transaction txn = env.beginTransaction(null, null);
        try {
            metaDb.put(txn, new DatabaseEntry(KEY_SNAPSHOT_INDEX), new DatabaseEntry(longToBytes(lastIncludedIndex)));
            metaDb.put(txn, new DatabaseEntry(KEY_SNAPSHOT_TERM), new DatabaseEntry(longToBytes(lastIncludedTerm)));
            metaDb.put(txn, new DatabaseEntry(KEY_SNAPSHOT_FILE),
                    new DatabaseEntry(fileName.getBytes(StandardCharsets.UTF_8)));
            // An older snapshot may still have its payload in here as a value.
            metaDb.delete(txn, new DatabaseEntry(KEY_SNAPSHOT_STATE));
            metaDb.put(txn, new DatabaseEntry(KEY_SNAPSHOT_CONFIG), new DatabaseEntry(configurationData));
            metaDb.put(txn, new DatabaseEntry(KEY_SNAPSHOT_SESSIONS), new DatabaseEntry(sessionData));

            try (Cursor cursor = logDb.openCursor(txn, null)) {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry value = new DatabaseEntry();
                OperationStatus status = cursor.getFirst(key, value, LockMode.DEFAULT);
                while (status == OperationStatus.SUCCESS && bytesToLong(key.getData()) <= lastIncludedIndex) {
                    cursor.delete();
                    status = cursor.getNext(key, value, LockMode.DEFAULT);
                }
            }
            txn.commit();
        } catch (RuntimeException e) {
            txn.abort();
            throw e;
        }
        String previousFile = this.snapshotFileName;
        this.snapshotFileName = fileName;
        if (previousFile != null && !previousFile.equals(fileName)) {
            new File(snapshotDir, previousFile).delete();
        }
        this.snapshotIndex = lastIncludedIndex;
        this.snapshotTerm = lastIncludedTerm;
        if (lastLogIndex <= lastIncludedIndex) {
            // Nothing was left beyond the snapshot boundary -- it's now the
            // effective "last entry" until something new is appended.
            this.lastLogIndex = lastIncludedIndex;
            this.lastLogTerm = lastIncludedTerm;
        }
    }

    @Override
    public synchronized long getTermAt(long index) {
        if (index <= 0) return 0L;
        if (index == snapshotIndex) return snapshotTerm;
        LogEntry entry = getLogEntry(index);
        return entry == null ? -1L : entry.getTerm();
    }

    private byte[] readBytes(byte[] key) {
        DatabaseEntry value = new DatabaseEntry();
        OperationStatus status = metaDb.get(null, new DatabaseEntry(key), value, LockMode.DEFAULT);
        return status == OperationStatus.SUCCESS ? value.getData() : new byte[0];
    }

    private static LogEntry parseEntry(byte[] data) {
        try {
            return LogEntry.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("corrupt log entry on disk", e);
        }
    }

    private static byte[] longToBytes(long v) {
        byte[] b = new byte[8];
        for (int i = 7; i >= 0; i--) {
            b[i] = (byte) (v & 0xFF);
            v >>= 8;
        }
        return b;
    }

    private static long bytesToLong(byte[] b) {
        long v = 0;
        for (int i = 0; i < 8; i++) {
            v = (v << 8) | (b[i] & 0xFF);
        }
        return v;
    }

    @Override
    public void close() {
        syncExecutor.shutdown();
        logDb.close();
        metaDb.close();
        env.close();
    }
}
