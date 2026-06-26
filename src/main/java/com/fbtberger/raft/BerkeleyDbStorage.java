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

    public BerkeleyDbStorage(File dataDir) {
        if (!dataDir.exists() && !dataDir.mkdirs()) {
            throw new IllegalStateException("could not create data dir: " + dataDir);
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
        try (Cursor cursor = logDb.openCursor(txn, null)) {
            DatabaseEntry key = new DatabaseEntry(longToBytes(fromIndexInclusive));
            DatabaseEntry value = new DatabaseEntry();
            OperationStatus status = cursor.getSearchKeyRange(key, value, LockMode.DEFAULT);
            while (status == OperationStatus.SUCCESS) {
                cursor.delete();
                status = cursor.getNext(key, value, LockMode.DEFAULT);
            }
            txn.commit();
        } catch (RuntimeException e) {
            txn.abort();
            throw e;
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
        byte[] stateData = readBytes(KEY_SNAPSHOT_STATE);
        byte[] configData = readBytes(KEY_SNAPSHOT_CONFIG);
        return new Snapshot(snapshotIndex, snapshotTerm, stateData, configData);
    }

    /**
     * Persists the snapshot's metadata and payload, then -- in the same
     * transaction, so the two never disagree even across a crash -- deletes
     * every log entry at or before {@code snapshot.lastIncludedIndex}. Any
     * entries already in the log beyond that index are left exactly as they
     * were; only a prefix is ever discarded.
     */
    @Override
    public synchronized void saveSnapshotAndCompact(Snapshot snapshot) {
        Transaction txn = env.beginTransaction(null, null);
        try {
            metaDb.put(txn, new DatabaseEntry(KEY_SNAPSHOT_INDEX), new DatabaseEntry(longToBytes(snapshot.lastIncludedIndex)));
            metaDb.put(txn, new DatabaseEntry(KEY_SNAPSHOT_TERM), new DatabaseEntry(longToBytes(snapshot.lastIncludedTerm)));
            metaDb.put(txn, new DatabaseEntry(KEY_SNAPSHOT_STATE), new DatabaseEntry(snapshot.stateMachineData));
            metaDb.put(txn, new DatabaseEntry(KEY_SNAPSHOT_CONFIG), new DatabaseEntry(snapshot.configurationData));

            try (Cursor cursor = logDb.openCursor(txn, null)) {
                DatabaseEntry key = new DatabaseEntry();
                DatabaseEntry value = new DatabaseEntry();
                OperationStatus status = cursor.getFirst(key, value, LockMode.DEFAULT);
                while (status == OperationStatus.SUCCESS && bytesToLong(key.getData()) <= snapshot.lastIncludedIndex) {
                    cursor.delete();
                    status = cursor.getNext(key, value, LockMode.DEFAULT);
                }
            }
            txn.commit();
        } catch (RuntimeException e) {
            txn.abort();
            throw e;
        }
        this.snapshotIndex = snapshot.lastIncludedIndex;
        this.snapshotTerm = snapshot.lastIncludedTerm;
        if (lastLogIndex <= snapshot.lastIncludedIndex) {
            // Nothing was left beyond the snapshot boundary -- it's now the
            // effective "last entry" until something new is appended.
            this.lastLogIndex = snapshot.lastIncludedIndex;
            this.lastLogTerm = snapshot.lastIncludedTerm;
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
