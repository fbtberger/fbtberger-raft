/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
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

        public Snapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] stateMachineData, byte[] configurationData) {
            this.lastIncludedIndex = lastIncludedIndex;
            this.lastIncludedTerm = lastIncludedTerm;
            this.stateMachineData = stateMachineData;
            this.configurationData = configurationData;
        }
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
