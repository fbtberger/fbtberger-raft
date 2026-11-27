/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.LogEntry;

import java.util.concurrent.CompletableFuture;

/**
 * Abstraction over the durable state every Raft server must keep: currentTerm,
 * votedFor, and the log (Figure 2). {@link RaftNode} talks only to this interface,
 * never to a specific storage engine, so the persistence layer can be swapped out
 * (Berkeley DB, an in-memory stand-in for tests, some other embedded store) without
 * touching the algorithm itself.
 * <p>
 * The correctness of the algorithm depends on this state actually being durable:
 * once a method here returns, the corresponding fact (a vote cast, a term entered,
 * an entry appended) must be able to survive an immediate crash, or the safety
 * arguments in §5.2/§5.3/§5.4 no longer hold. An implementation that doesn't
 * actually persist anything (see {@link InMemoryStorage}) is fine for tests and
 * demos, but is not safe to run a real cluster on.
 */
public interface RaftStorage extends AutoCloseable {

    /** The latest term this server has seen, or 0 if it has never persisted one. */
    long getCurrentTerm();

    /** The candidateId this server voted for in {@link #getCurrentTerm()}, or null if none. */
    String getVotedFor();

    /**
     * Persists a new currentTerm together with votedFor in one atomic step. Used
     * whenever the two must change together: starting an election bumps the term
     * and votes for self in the same step (§5.2); stepping down because of a newer
     * term clears votedFor for that new term.
     */
    void setTermAndVote(long term, String votedFor);

    /** The log entry at the given 1-based index, or null if there is none. */
    LogEntry getLogEntry(long index);

    /** The index of the last entry in the log, or 0 if the log is empty and no snapshot has ever been taken or installed. If the log is empty only because everything in it has been compacted away by a snapshot (§7), returns that snapshot's lastIncludedIndex instead. */
    long getLastLogIndex();

    /** The term of the last entry in the log, with the same snapshot fallback as {@link #getLastLogIndex()}. */
    long getLastLogTerm();

    /**
     * Appends entries to the log. Callers are responsible for resolving any
     * conflicts (via {@link #truncateFrom}) before calling this, so the entries
     * passed in always extend the log rather than overwrite it in place.
     */
    void appendEntries(Iterable<LogEntry> entries);

    /**
     * §10.2.1: appends entries so they are immediately readable (for
     * replication) but defers the durable fsync. Returns a future that
     * completes once the data is safely on disk. Leaders use this to
     * write to their own disk in parallel with replicating to followers.
     * The default implementation falls back to synchronous
     * {@link #appendEntries}.
     */
    default CompletableFuture<Void> appendEntriesDeferSync(Iterable<LogEntry> entries) {
        appendEntries(entries);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Deletes the entry at fromIndexInclusive and everything after it. This is how
     * a follower discards entries that conflict with the leader's log (Figure 2,
     * AppendEntries rule 3); leaders themselves never delete from their own log
     * (the Leader Append-Only property, Figure 3).
     */
    void truncateFrom(long fromIndexInclusive);

    // ---- snapshotting (§7) --------------------------------------------

    /**
     * Bundles everything a snapshot needs to carry: the Raft-level boundary
     * it covers (lastIncludedIndex/Term, Figure 13) plus two opaque payloads
     * -- whatever {@link StateMachine#takeSnapshot()} produced, and the
     * cluster's {@code ClusterConfiguration} (§6) as of that index, serialized.
     * Bundling the configuration in here too (rather than leaving it purely
     * up to the state machine) is what lets a configuration log entry be
     * compacted away by §7 without {@link RaftNode} losing track of §6
     * membership on restart -- see
     * {@code RaftNode.recomputeEffectiveConfiguration} for how it's used.
     */
    final class Snapshot {
        public final long lastIncludedIndex;
        public final long lastIncludedTerm;
        public final byte[] stateMachineData;
        public final byte[] configurationData;
        /**
         * The client session table as of this index ({@code ClientSessions#toBytes}),
         * which is replicated state just as the state machine's data is: without it a
         * server restored from this snapshot would apply a retried command a second
         * time. Never null; empty when no client has used a session, and empty for
         * every snapshot stored before this field existed.
         */
        public final byte[] sessionData;

        public Snapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] stateMachineData, byte[] configurationData) {
            this(lastIncludedIndex, lastIncludedTerm, stateMachineData, configurationData, new byte[0]);
        }

        public Snapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] stateMachineData,
                        byte[] configurationData, byte[] sessionData) {
            this.lastIncludedIndex = lastIncludedIndex;
            this.lastIncludedTerm = lastIncludedTerm;
            this.stateMachineData = stateMachineData;
            this.configurationData = configurationData;
            this.sessionData = sessionData == null ? new byte[0] : sessionData;
        }
    }

    // ---- streaming snapshots -----------------------------------------
    //
    // The four methods below are how RaftNode reads and writes snapshots. Their
    // defaults are built on getSnapshot()/saveSnapshotAndCompact(), so a backend
    // that only implements those keeps working and holds the payload in memory;
    // WalStorage and BerkeleyDbStorage override them and keep it in a file.

    /** Everything about a snapshot except the state machine's bytes. */
    final class SnapshotMeta {
        public final long lastIncludedIndex;
        public final long lastIncludedTerm;
        public final byte[] configurationData;
        public final byte[] sessionData;
        /** Length in bytes of the state machine payload. */
        public final long dataLength;

        public SnapshotMeta(long lastIncludedIndex, long lastIncludedTerm, byte[] configurationData,
                            byte[] sessionData, long dataLength) {
            this.lastIncludedIndex = lastIncludedIndex;
            this.lastIncludedTerm = lastIncludedTerm;
            this.configurationData = configurationData;
            this.sessionData = sessionData == null ? new byte[0] : sessionData;
            this.dataLength = dataLength;
        }
    }

    /**
     * A snapshot being written. Write the state machine's bytes to {@link #stream()},
     * then {@link #commit}; {@link #close()} without a commit discards what was written.
     * Nothing about the stored snapshot or the log changes before {@code commit}.
     */
    interface SnapshotSink extends AutoCloseable {
        java.io.OutputStream stream();

        /**
         * Makes this the stored snapshot and discards every log entry at or before its
         * index, exactly as {@link #saveSnapshotAndCompact} does -- including that a
         * snapshot not newer than the stored one is dropped.
         */
        void commit(byte[] configurationData, byte[] sessionData) throws java.io.IOException;

        @Override
        void close();
    }

    /** The stored snapshot without its payload, or null if there is none. Cheap. */
    default SnapshotMeta getSnapshotMeta() {
        Snapshot snapshot = getSnapshot();
        return snapshot == null ? null : new SnapshotMeta(snapshot.lastIncludedIndex, snapshot.lastIncludedTerm,
                snapshot.configurationData, snapshot.sessionData, snapshot.stateMachineData.length);
    }

    /** Starts writing a snapshot that covers the log through {@code lastIncludedIndex}. */
    default SnapshotSink beginSnapshot(long lastIncludedIndex, long lastIncludedTerm) throws java.io.IOException {
        java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
        return new SnapshotSink() {
            @Override public java.io.OutputStream stream() { return buffer; }

            @Override public void commit(byte[] configurationData, byte[] sessionData) {
                saveSnapshotAndCompact(new Snapshot(lastIncludedIndex, lastIncludedTerm,
                        buffer.toByteArray(), configurationData, sessionData));
            }

            @Override public void close() { }
        };
    }

    /**
     * The stored snapshot's state machine payload as a stream, or null if there is no
     * snapshot. The caller closes it. The stream stays valid if a newer snapshot is
     * stored while it is being read.
     */
    default java.io.InputStream openSnapshotData() throws java.io.IOException {
        Snapshot snapshot = getSnapshot();
        return snapshot == null ? null : new java.io.ByteArrayInputStream(snapshot.stateMachineData);
    }

    /**
     * Reads part of the stored snapshot's payload: up to {@code length} bytes from
     * {@code offset} into {@code target}. This is what a leader sends a follower one
     * chunk at a time.
     *
     * @return the number of bytes read, or -1 if the stored snapshot is no longer the one
     *         at {@code lastIncludedIndex} -- the transfer then starts over with the new one
     */
    default int readSnapshotData(long lastIncludedIndex, long offset, byte[] target, int targetOffset, int length)
            throws java.io.IOException {
        Snapshot snapshot = getSnapshot();
        if (snapshot == null || snapshot.lastIncludedIndex != lastIncludedIndex) {
            return -1;
        }
        int available = (int) Math.max(0, Math.min(length, snapshot.stateMachineData.length - offset));
        System.arraycopy(snapshot.stateMachineData, (int) offset, target, targetOffset, available);
        return available;
    }

    /** The lastIncludedIndex of the most recently saved snapshot, or 0 if this server has never taken or installed one. Cheap -- prefer this over {@link #getSnapshot()} when only the boundary is needed. */
    long getSnapshotIndex();

    /** The lastIncludedTerm of the most recently saved snapshot, or 0 if none. */
    long getSnapshotTerm();

    /**
     * The full most recently saved snapshot, or null if none has ever been
     * taken or installed. Heavier than {@link #getSnapshotIndex()} /
     * {@link #getSnapshotTerm()} since it loads the actual payload bytes;
     * only call this when the payload itself is needed -- restoring state
     * at startup, or a leader assembling an {@code InstallSnapshot} request
     * for a lagging follower.
     */
    Snapshot getSnapshot();

    /**
     * Atomically persists {@code snapshot} and discards every log entry at
     * or before {@code snapshot.lastIncludedIndex} -- the compaction half of
     * §7. The caller is responsible for only ever snapshotting through an
     * index that has already been applied to the state machine; entries
     * after that index, if any, are left untouched.
     */
    void saveSnapshotAndCompact(Snapshot snapshot);

    /**
     * The term of the entry at {@code index}, whether it's still physically
     * present in the log or was compacted away into the current snapshot:
     * 0 for index 0, {@link #getSnapshotTerm()} if index equals
     * {@link #getSnapshotIndex()}, or the stored entry's term otherwise.
     * Returns -1 if {@code index} names neither of those -- an entry older
     * than the snapshot boundary, or one past the end of the log -- which
     * callers should treat the same as "no such entry".
     */
    long getTermAt(long index);

    @Override
    void close();
}
