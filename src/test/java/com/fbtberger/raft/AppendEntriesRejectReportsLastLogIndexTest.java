/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * v118 (follower half) — a rejected AppendEntries must report the follower's own last log index,
 * so the leader can back a stale-high matchIndex down instead of pinning nextIndex above the
 * snapshot boundary forever.
 *
 * <p>These drive {@link RaftNode#handleAppendEntries} directly (no election, no transport), the
 * same deterministic style as {@code SnapshotCatchUpGateTest}.
 */
class AppendEntriesRejectReportsLastLogIndexTest {

    private RaftNode follower;

    @AfterEach
    void tearDown() {
        if (follower != null) follower.shutdown();
    }

    /** A wiped/empty peer: the leader probes high (as after a recreate); the reject must say 0. */
    @Test
    void anEmptyFollowerRejectsWithConflictHintZero() throws Exception {
        follower = new RaftNode(config("f1"), new InMemoryStorage(),
                new KeyValueStateMachine(), addr -> null, RaftMetrics.noop());

        AppendEntriesResponse r = follower.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("leader")
                .setPrevLogIndex(730).setPrevLogTerm(1).setLeaderCommit(0).build());

        assertFalse(r.getSuccess(), "an empty follower cannot satisfy prevLogIndex=730");
        assertEquals(0, r.getConflictLastLogIndex(), "an empty log's last index is 0");
    }

    /** A shorter follower reports exactly how far it reaches when the leader probes beyond it. */
    @Test
    void aShorterFollowerReportsItsOwnLastIndex() throws Exception {
        follower = new RaftNode(config("f2"), new InMemoryStorage(),
                new KeyValueStateMachine(), addr -> null, RaftMetrics.noop());

        // Seed entries 1..3 (accepted from an empty log at prevLogIndex 0).
        AppendEntriesRequest.Builder seed = AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("leader").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0);
        for (int i = 1; i <= 3; i++) {
            seed.addEntries(LogEntry.newBuilder().setIndex(i).setTerm(1)
                    .setCommand(ByteString.copyFromUtf8("SET k" + i + " " + i)));
        }
        assertTrue(follower.handleAppendEntries(seed.build()).getSuccess());

        // Leader probes at index 10 (beyond our log) → reject, hint = our last index (3).
        AppendEntriesResponse r = follower.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("leader")
                .setPrevLogIndex(10).setPrevLogTerm(1).setLeaderCommit(0).build());

        assertFalse(r.getSuccess());
        assertEquals(3, r.getConflictLastLogIndex());
    }

    private static RaftConfig config(String selfId) throws Exception {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", selfId);
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-hint-unused/" + selfId);
        props.setProperty("peer." + selfId, "localhost:9091");
        props.setProperty("peer.other1", "localhost:9092");
        props.setProperty("peer.other2", "localhost:9093");
        props.setProperty("snapshot.threshold", "100000");

        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-hint-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }
}
