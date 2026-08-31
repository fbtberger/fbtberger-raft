/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Compaction discards the log in batches, so appending does not have to wait for it.
 *
 * <p>It used to be one synchronized method holding one transaction across every deletion,
 * and {@code appendEntries} is synchronized on the same monitor: the log could not grow
 * while the log was being compacted. The cost is proportional to the entries discarded,
 * which makes it a function of the snapshot threshold rather than of anything the protocol
 * bounds. Measured on the Pi cluster at a threshold of 20000: 450 ms, heartbeats failing
 * to all four peers, the leader stepping down, two elections in half a second -- against
 * an election timeout of 300 ms.
 *
 * <p>Splitting it means the boundary is committed first and the entries below it are
 * removed afterwards. That trade is deliberate and it is the reason for the second test
 * here: a process that dies in between leaves entries behind, and they must not be
 * mistaken for the end of the log.
 */
class BerkeleyDbCompactionTest {

    @Test
    void anAppendDoesNotWaitForTheWholeCompaction(@TempDir Path dir) throws Exception {
        try (BerkeleyDbStorage store = new BerkeleyDbStorage(dir.toFile())) {
            store.appendEntries(entries(1, 4_000));

            CountDownLatch appendDone = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicLong appendFinishedAtBatch = new AtomicLong(-1);

            Thread compactor = new Thread(() -> store.saveSnapshotAndCompact(
                    new RaftStorage.Snapshot(4_000, 1, new byte[] {1}, new byte[] {2})), "compactor");

            Thread appender = new Thread(() -> {
                try {
                    // Waits on the monitor for at most one batch, not for all eight.
                    store.appendEntries(entries(4_001, 4_001));
                    appendFinishedAtBatch.set(store.getSnapshotIndex());
                } catch (Throwable t) {
                    failure.set(t);
                } finally {
                    appendDone.countDown();
                }
            }, "appender");

            compactor.start();
            appender.start();

            assertTrue(appendDone.await(10, TimeUnit.SECONDS), "the append never completed");
            compactor.join(10_000);
            assertNull(failure.get(), String.valueOf(failure.get()));

            // What the split guarantees: the appended entry is there, and everything the
            // snapshot covers is gone. Not asserted by timing -- a wall-clock threshold
            // would pass on a fast disk whether or not the monitor was ever released.
            assertNotNull(store.getLogEntry(4_001), "the append was lost");
            assertNull(store.getLogEntry(1), "entry 1 should have been compacted away");
            assertNull(store.getLogEntry(4_000), "the boundary entry should have been compacted away");
            assertEquals(4_001, store.getLastLogIndex());
            assertEquals(4_000, store.getSnapshotIndex());
        }
    }

    /**
     * A crash between the boundary and the deletion leaves entries behind, and the next
     * compaction sweeps them.
     *
     * <p>Deletion runs from the first key upwards, so what an interrupted compaction leaves
     * are the keys just below the boundary -- the log still ends exactly where the boundary
     * is, which is why this case needs no rewind protection. What it does need is for the
     * leftovers not to survive forever: the next compaction's cursor starts at the first
     * key, so they go with the entries the newer boundary covers.
     */
    @Test
    void whatAnInterruptedCompactionLeavesIsSweptByTheNextOne(@TempDir Path dir) throws Exception {
        try (BerkeleyDbStorage store = new BerkeleyDbStorage(dir.toFile())) {
            store.appendEntries(entries(1, 10));
            // Exactly the crash: the boundary is recorded, nothing is deleted.
            assertTrue(store.recordSnapshotBoundary(
                    new RaftStorage.Snapshot(10, 1, new byte[] {1}, new byte[] {2})));
            assertNotNull(store.getLogEntry(1), "nothing should have been deleted yet");
        }

        try (BerkeleyDbStorage reopened = new BerkeleyDbStorage(dir.toFile())) {
            assertEquals(10, reopened.getSnapshotIndex());
            assertEquals(10, reopened.getLastLogIndex());

            reopened.appendEntries(entries(11, 12));
            reopened.saveSnapshotAndCompact(new RaftStorage.Snapshot(12, 1, new byte[] {1}, new byte[] {2}));

            assertNull(reopened.getLogEntry(1), "a leftover from the interrupted compaction survived");
            assertNull(reopened.getLogEntry(10), "a leftover from the interrupted compaction survived");
            assertEquals(12, reopened.getLastLogIndex());
        }
    }

    /**
     * A snapshot boundary above everything stored locally must not be rewound on restart.
     *
     * <p>This is what {@code InstallSnapshot} produces: a follower far enough behind is sent
     * the leader's snapshot instead of entries, and its boundary lands beyond anything that
     * node has in its own log. In memory {@code saveSnapshotAndCompact} already carried the
     * log bounds forward to it. On restart nothing did -- {@code recoverCachedLogBounds}
     * took the last physical key and believed it -- so a reopened node reported a
     * {@code lastLogIndex} below its own {@code snapshotIndex}, a pair no invariant in this
     * class survives. It predates the batched compaction; splitting the method is what made
     * the recovery path worth reading closely.
     */
    @Test
    void aBoundaryBeyondTheStoredLogSurvivesARestart(@TempDir Path dir) throws Exception {
        try (BerkeleyDbStorage store = new BerkeleyDbStorage(dir.toFile())) {
            store.appendEntries(entries(1, 10));
            assertTrue(store.recordSnapshotBoundary(
                    new RaftStorage.Snapshot(20, 3, new byte[] {1}, new byte[] {2})));
            assertEquals(20, store.getLastLogIndex(), "in memory this was already right");
        }

        try (BerkeleyDbStorage reopened = new BerkeleyDbStorage(dir.toFile())) {
            assertEquals(20, reopened.getSnapshotIndex());
            assertEquals(20, reopened.getLastLogIndex(),
                    "the log cannot end below the snapshot that covers it");
            assertEquals(3, reopened.getLastLogTerm());
        }
    }

    private static List<LogEntry> entries(long from, long to) {
        List<LogEntry> entries = new ArrayList<>();
        for (long index = from; index <= to; index++) {
            entries.add(LogEntry.newBuilder()
                    .setIndex(index)
                    .setTerm(1)
                    .setCommand(ByteString.copyFrom("SET k " + index, StandardCharsets.UTF_8))
                    .build());
        }
        return entries;
    }
}
