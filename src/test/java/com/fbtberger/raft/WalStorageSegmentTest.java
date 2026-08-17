/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The parts of {@link WalStorage} that are genuinely its own -- segment rollover, truncating across
 * a segment boundary, recovering over several segment files, dropping segments that compaction has
 * emptied. The behaviour it shares with every other {@link RaftStorage} is asserted once, in
 * {@link RaftStorageContract}, via {@link WalStorageContractTest}.
 *
 * <p>A tiny {@code maxSegmentBytes} is used so the log rolls over after a handful of entries rather
 * than 64 MB of them.
 */
class WalStorageSegmentTest {

    private static final long TINY_SEGMENT = 128;

    @TempDir
    File tempDir;

    private WalStorage store;

    @BeforeEach
    void setUp() {
        store = new WalStorage(tempDir, TINY_SEGMENT);
    }

    @AfterEach
    void tearDown() {
        if (store != null) store.close();
    }

    @Test
    void theLogRollsOverIntoSeveralSegments() {
        store.appendEntries(range(1, 40, 1));

        assertTrue(segmentCount() > 1, "40 entries at " + TINY_SEGMENT + "B/segment must roll over");
        assertEquals(40, store.getLastLogIndex());
        assertNotNull(store.getLogEntry(1), "the first entry, in the first segment");
        assertNotNull(store.getLogEntry(40), "the last entry, in the active segment");
    }

    @Test
    void everyEntryIsStillReadableAfterRecoveringOverSeveralSegments() {
        store.appendEntries(range(1, 40, 3));
        store.close();

        store = new WalStorage(tempDir, TINY_SEGMENT);

        assertEquals(40, store.getLastLogIndex());
        assertEquals(3, store.getLastLogTerm());
        for (int i = 1; i <= 40; i++) {
            assertEquals("cmd-" + i, store.getLogEntry(i).getCommand().toStringUtf8(), "entry " + i);
        }
    }

    @Test
    void truncatingAcrossASegmentBoundaryDropsTheSegmentsAfterIt() {
        store.appendEntries(range(1, 40, 1));
        int before = segmentCount();

        store.truncateFrom(10);

        assertEquals(9, store.getLastLogIndex());
        assertNotNull(store.getLogEntry(9));
        assertNull(store.getLogEntry(10));
        assertNull(store.getLogEntry(40));
        assertTrue(segmentCount() < before, "segments past the truncation point must be deleted");
    }

    @Test
    void theWalKeepsWorkingAfterTruncatingAcrossASegmentBoundary() {
        // The follower catch-up sequence, but forced to cross a segment: truncate mid-file, then
        // take the leader's entries -- which must land in a segment that is open and writable.
        store.appendEntries(range(1, 40, 1));
        store.truncateFrom(10);

        store.appendEntries(range(10, 30, 7));

        assertEquals(30, store.getLastLogIndex());
        assertEquals(7, store.getLastLogTerm());
        assertEquals(1, store.getLogEntry(9).getTerm(), "kept from before the truncation");
        assertEquals(7, store.getLogEntry(10).getTerm(), "the leader's version");

        store.close();
        store = new WalStorage(tempDir, TINY_SEGMENT);

        assertEquals(30, store.getLastLogIndex(), "and it all survives a reopen");
        assertEquals(7, store.getLogEntry(30).getTerm());
    }

    @Test
    void compactionDeletesSegmentsItHasEmptied() {
        store.appendEntries(range(1, 40, 1));
        int before = segmentCount();

        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(35, 1, new byte[0], new byte[0]));

        assertTrue(segmentCount() < before, "segments with no live entries left must be deleted");
        assertNull(store.getLogEntry(35), "compacted away");
        assertNotNull(store.getLogEntry(36), "past the boundary -- kept");
        assertEquals(40, store.getLastLogIndex());
    }

    private int segmentCount() {
        File[] segments = tempDir.listFiles((dir, name) -> name.matches("wal-\\d{6}\\.log"));
        return segments == null ? 0 : segments.length;
    }

    private static List<LogEntry> range(int fromInclusive, int toInclusive, long term) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = fromInclusive; i <= toInclusive; i++) {
            entries.add(LogEntry.newBuilder()
                    .setIndex(i)
                    .setTerm(term)
                    .setCommand(ByteString.copyFromUtf8("cmd-" + i))
                    .build());
        }
        return entries;
    }
}
