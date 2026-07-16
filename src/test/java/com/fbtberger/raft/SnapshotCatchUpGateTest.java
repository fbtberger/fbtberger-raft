/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * v116 — a follower (or learner) must not take a snapshot, and compact its log, while it is
 * still catching up to the leader.
 *
 * <h2>The bug this pins down</h2>
 * {@code applyCommittedEntries()} checks the snapshot threshold after EVERY batch it applies —
 * including the partial batches a lagging node receives while catching up. {@code commitIndex}
 * is capped at the last entry actually stored in the batch
 * ({@code Math.min(leaderCommit, lastNewIndex)}), so the first batch that crosses the threshold
 * can snapshot at an intermediate index and discard the log up to it, while the leader has
 * committed far beyond that point and the remaining entries have not yet arrived.
 *
 * <p>The fix gates the snapshot on {@code appliedIndex() >= leaderCommitSeen()}: only compact
 * once level with what the leader says is committed. That is the caught-up predicate from
 * {@link RaftNode#isCaughtUp()} (v105) without the lease clause — a lagging lease must not stop a
 * learner from compacting, a lagging <i>apply</i> must.
 *
 * <h2>Why the assertion counts {@code prepareCowSnapshot()} calls</h2>
 * The snapshot itself runs on a background thread (COW, §5.1), so {@code snapshotIndex()} advances
 * asynchronously and racily — a poor thing to assert on. But {@code RaftNode} calls
 * {@link StateMachine#prepareCowSnapshot()} SYNCHRONOUSLY under the Raft lock, in the same breath
 * as deciding to snapshot. Counting those calls is therefore a deterministic signal of the
 * <i>decision</i>, independent of when the async compaction lands.
 */
class SnapshotCatchUpGateTest {

    private RaftNode follower;

    @AfterEach
    void tearDown() {
        if (follower != null) follower.shutdown();
    }

    @Test
    void aFollowerDoesNotSnapshotWhileStillBehindTheLeadersCommit() throws Exception {
        CountingStateMachine sm = new CountingStateMachine();
        // Threshold 5: six applied entries would normally be enough to trigger a snapshot.
        follower = new RaftNode(configWithThreshold("f1", 5), new InMemoryStorage(),
                sm, addr -> null, RaftMetrics.noop());

        // Batch 1: entries 1..6 arrive, but the leader has already committed through 12 (it is far
        // ahead — 7..12 simply have not reached us yet). commitIndex is capped at 6, so we apply
        // 1..6 — enough to cross the threshold of 5 — yet we are still 6 short of the leader.
        AppendEntriesResponse r1 = follower.handleAppendEntries(batch(1, 6, /*prevIdx*/ 0, /*leaderCommit*/ 12));
        assertTrue(r1.getSuccess());
        assertEquals(6, follower.appliedIndex());

        // The threshold is crossed, but we are behind: NO snapshot may be taken here, or the log
        // through 6 would be discarded before 7..12 have even been applied.
        assertEquals(0, sm.cowSnapshots(),
                "a follower must not compact while still catching up to the leader's commit");

        // Batch 2: entries 7..12 arrive; now appliedIndex reaches the leader's committed 12.
        AppendEntriesResponse r2 = follower.handleAppendEntries(batch(7, 12, /*prevIdx*/ 6, /*leaderCommit*/ 12));
        assertTrue(r2.getSuccess());
        assertEquals(12, follower.appliedIndex());

        // Caught up — the deferred snapshot is now allowed to fire, exactly once. This half guards
        // against a "fix" that simply never snapshots on a follower.
        assertEquals(1, sm.cowSnapshots(),
                "once level with the leader, the deferred snapshot must be taken");
    }

    /** One AppendEntries carrying entries [from..to] (term 1) after {@code prevIdx}, with the given leaderCommit. */
    private static AppendEntriesRequest batch(int from, int to, long prevIdx, long leaderCommit) {
        AppendEntriesRequest.Builder b = AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("leader")
                .setPrevLogIndex(prevIdx).setPrevLogTerm(prevIdx == 0 ? 0 : 1)
                .setLeaderCommit(leaderCommit);
        for (int i = from; i <= to; i++) {
            b.addEntries(LogEntry.newBuilder().setIndex(i).setTerm(1)
                    .setCommand(ByteString.copyFromUtf8("SET k" + i + " " + i)));
        }
        return b.build();
    }

    private static RaftConfig configWithThreshold(String selfId, int threshold) throws Exception {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", selfId);
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-catchup-unused/" + selfId);
        props.setProperty("peer." + selfId, "localhost:9091");
        props.setProperty("peer.other1", "localhost:9092");
        props.setProperty("peer.other2", "localhost:9093");
        props.setProperty("snapshot.threshold", Integer.toString(threshold));

        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-catchup-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    /**
     * Delegates to a real {@link KeyValueStateMachine} but counts how often the node decided to
     * snapshot — i.e. how often it synchronously asked for a copy-on-write capture.
     */
    private static final class CountingStateMachine implements StateMachine {
        private final KeyValueStateMachine delegate = new KeyValueStateMachine();
        private final AtomicInteger cowSnapshots = new AtomicInteger();

        int cowSnapshots() {
            return cowSnapshots.get();
        }

        @Override
        public byte[] apply(byte[] command) {
            return delegate.apply(command);
        }

        @Override
        public byte[] takeSnapshot() {
            return delegate.takeSnapshot();
        }

        @Override
        public void restoreSnapshot(byte[] snapshot) {
            delegate.restoreSnapshot(snapshot);
        }

        @Override
        public Supplier<byte[]> prepareCowSnapshot() {
            cowSnapshots.incrementAndGet();
            return delegate.prepareCowSnapshot();
        }
    }
}
