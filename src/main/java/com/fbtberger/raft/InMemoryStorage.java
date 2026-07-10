/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.LogEntry;

import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * {@link RaftStorage} that keeps everything in a plain in-memory map and forgets it
 * all on shutdown. Useful for unit tests and quick local experiments where you don't
 * want to stand up a Berkeley DB environment, or want a crashed/restarted node to
 * genuinely lose its state.
 * <p>
 * <b>This implementation does not satisfy Raft's durability requirement</b> (Figure
 * 2: currentTerm, votedFor, and log must survive a crash) and must not be used for a
 * real cluster -- a node using it that crashes and restarts may re-vote in a term it
 * already voted in, or "forget" log entries it had acknowledged to a leader, both of
 * which the safety arguments in §5.2-§5.4 depend on never happening.
 */
public final class InMemoryStorage implements RaftStorage {

    private final Object lock = new Object();
    private final NavigableMap<Long, LogEntry> log = new TreeMap<>();

    private long currentTerm = 0L;
    private String votedFor = null;
    private Snapshot snapshot = null;

    @Override
    public long getCurrentTerm() {
        synchronized (lock) {
            return currentTerm;
        }
    }

    @Override
    public String getVotedFor() {
        synchronized (lock) {
            return votedFor;
        }
    }

    @Override
    public void setTermAndVote(long term, String votedFor) {
        synchronized (lock) {
            this.currentTerm = term;
            this.votedFor = votedFor;
        }
    }

    @Override
    public LogEntry getLogEntry(long index) {
        synchronized (lock) {
            return log.get(index);
        }
    }

    @Override
    public long getLastLogIndex() {
        synchronized (lock) {
            if (!log.isEmpty()) return log.lastKey();
            return snapshot == null ? 0L : snapshot.lastIncludedIndex;
        }
    }

    @Override
    public long getLastLogTerm() {
        synchronized (lock) {
            if (!log.isEmpty()) return log.lastEntry().getValue().getTerm();
            return snapshot == null ? 0L : snapshot.lastIncludedTerm;
        }
    }

    @Override
    public void appendEntries(Iterable<LogEntry> entries) {
        synchronized (lock) {
            for (LogEntry entry : entries) {
                log.put(entry.getIndex(), entry);
            }
        }
    }

    @Override
    public void truncateFrom(long fromIndexInclusive) {
        synchronized (lock) {
            log.tailMap(fromIndexInclusive, true).clear();
        }
    }

    @Override
    public long getSnapshotIndex() {
        synchronized (lock) {
            return snapshot == null ? 0L : snapshot.lastIncludedIndex;
        }
    }

    @Override
    public long getSnapshotTerm() {
        synchronized (lock) {
            return snapshot == null ? 0L : snapshot.lastIncludedTerm;
        }
    }

    @Override
    public Snapshot getSnapshot() {
        synchronized (lock) {
            return snapshot;
        }
    }

    @Override
    public void saveSnapshotAndCompact(Snapshot snapshot) {
        synchronized (lock) {
            // The snapshot boundary must never move backwards. A background
            // (COW) snapshot in RaftNode does its "is this still worth saving?"
            // check off-lock and then saves here; if a newer snapshot -- e.g.
            // one just installed by InstallSnapshot, possibly after a
            // step-down/re-election -- committed in between, that stale save
            // must be dropped rather than clobbering the higher boundary.
            // Making the check-and-set atomic under this monitor closes that
            // race for every caller.
            if (this.snapshot != null && snapshot.lastIncludedIndex <= this.snapshot.lastIncludedIndex) {
                return;
            }
            this.snapshot = snapshot;
            // Removes every entry at or before lastIncludedIndex; anything
            // newer, if the caller already had some, is left untouched.
            log.headMap(snapshot.lastIncludedIndex, true).clear();
        }
    }

    @Override
    public long getTermAt(long index) {
        synchronized (lock) {
            if (index <= 0) return 0L;
            if (snapshot != null && index == snapshot.lastIncludedIndex) return snapshot.lastIncludedTerm;
            LogEntry entry = log.get(index);
            return entry == null ? -1L : entry.getTerm();
        }
    }

    @Override
    public void close() {
        // Nothing to release; state simply disappears, as documented above.
    }
}
