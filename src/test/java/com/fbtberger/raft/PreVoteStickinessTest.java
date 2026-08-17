/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.PreVoteRequest;
import com.fbtberger.raft.proto.PreVoteResponse;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import com.fbtberger.raft.transport.RaftTransport;
import com.fbtberger.raft.transport.RaftTransportFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * v122 / issue #2 -- leader stickiness (&sect;4.2.3) must actually hold on the node it matters for.
 *
 * <h2>The bug</h2>
 * Every restart of any voter took the leadership away from a healthy incumbent, about 500 ms after
 * the restarting node came up -- three restarts, three takeovers, no exceptions.
 *
 * <p>Cause: {@code lastLeaderContactMs} is written only by {@code handleAppendEntries}, and a
 * leader never receives one. Its value therefore stays frozen at whatever it was before the node
 * won its term, so {@code hasLeaderStickiness()} was permanently false <i>on the leader</i>. With
 * three voters the quorum is two and a candidate counts itself, so a single foreign grant decides
 * the round -- and the incumbent leader supplied it, every time. Healthy followers refused
 * correctly; they were simply outnumbered by the one node that should have refused hardest.
 *
 * <p>A freshly started node had the mirror-image problem: the field initialises to 0, i.e. "last
 * heard from a leader in 1970", so it handed out grants before it had listened for the incumbent
 * even once. During a rolling restart that is the second grant.
 *
 * <p>These tests drive {@code handlePreVote} directly -- it is the responder decision under test,
 * and no timing is involved in reaching it.
 */
class PreVoteStickinessTest {

    private static final Map<String, String> ADDRESS_OF = Map.of(
            "n1", "localhost:7301", "n2", "localhost:7302", "n3", "localhost:7303");

    private RaftNode node;
    private InMemoryStorage store;

    @BeforeEach
    void boot() {
        store = new InMemoryStorage();
        node = new RaftNode(configFor("n1"), store, new KeyValueStateMachine(),
                new GrantingPeers(), RaftMetrics.noop());
        // No start(): the background election timer would race these assertions. Tests that need
        // the startup path call it explicitly.
    }

    @AfterEach
    void tearDown() {
        if (node != null) node.shutdown();
    }

    /**
     * THE REGRESSION. A sitting leader must refuse to help a challenger unseat it.
     *
     * <p>Pre-fix this granted, and that single grant was the whole quorum.
     */
    @Test
    void aSittingLeaderDeniesPreVotes() {
        node.startElection();
        assertEquals(ServerRole.LEADER, node.role(), "precondition: the node should have won");

        PreVoteResponse response = node.handlePreVote(preVoteFrom("n2", store.getCurrentTerm() + 1));

        assertFalse(response.getVoteGranted(),
                "a leader must not grant a pre-vote against itself");
    }

    /** A node that has only just come up has not yet listened for the incumbent. */
    @Test
    void aFreshlyStartedFollowerDeniesPreVotes() {
        node.start();

        PreVoteResponse response = node.handlePreVote(preVoteFrom("n2", store.getCurrentTerm() + 1));

        assertFalse(response.getVoteGranted(),
                "a node that just started must not grant before hearing from the leader");
    }

    /**
     * The converse, so the guards above cannot be satisfied by refusing everything: a follower
     * that genuinely has not heard from anyone must still grant, or no election can ever start.
     */
    @Test
    void aFollowerWithoutRecentLeaderContactStillGrants() {
        // Never started, never contacted: lastLeaderContactMs is still 0.
        PreVoteResponse response = node.handlePreVote(preVoteFrom("n2", store.getCurrentTerm() + 1));

        assertTrue(response.getVoteGranted(),
                "without leader contact a follower must grant, else the cluster cannot recover");
    }

    /** Stickiness must not override the log check -- a behind candidate is refused regardless. */
    @Test
    void aCandidateWithAStaleLogIsDeniedEvenWithoutStickiness() {
        store.appendEntries(java.util.List.of(com.fbtberger.raft.proto.LogEntry.newBuilder()
                .setTerm(5).setIndex(1).build()));

        PreVoteResponse response = node.handlePreVote(PreVoteRequest.newBuilder()
                .setCandidateId("n2").setTerm(store.getCurrentTerm() + 1)
                .setLastLogIndex(0).setLastLogTerm(0).build());

        assertFalse(response.getVoteGranted(), "a candidate behind our log must never be granted");
    }

    // -- Harness ---------------------------------------------------------------

    private static PreVoteRequest preVoteFrom(String candidate, long term) {
        return PreVoteRequest.newBuilder()
                .setCandidateId(candidate).setTerm(term)
                .setLastLogIndex(0).setLastLogTerm(0).build();
    }

    private static RaftConfig configFor(String selfId) {
        try {
            java.util.Properties props = new java.util.Properties();
            props.setProperty("node.id", selfId);
            props.setProperty("node.port", "7301");
            props.setProperty("data.dir", "/tmp/raft-stickiness-unused/" + selfId);
            ADDRESS_OF.forEach((id, addr) -> props.setProperty("peer." + id, addr));
            java.nio.file.Path tmp =
                    java.nio.file.Files.createTempFile("raft-stickiness-", ".properties");
            try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
                props.store(out, null);
            }
            return RaftConfig.load(tmp);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Grants everything inline, so this node can be made leader without any timing. */
    private static final class GrantingPeers implements RaftTransportFactory {
        @Override
        public RaftTransport connect(String address) {
            return new RaftTransport() {
                @Override
                public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest r) {
                    return CompletableFuture.completedFuture(PreVoteResponse.newBuilder()
                            .setTerm(0).setVoteGranted(true).build());
                }

                @Override
                public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest r) {
                    return CompletableFuture.completedFuture(RequestVoteResponse.newBuilder()
                            .setTerm(r.getTerm()).setVoteGranted(true).build());
                }

                @Override
                public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest r) {
                    return CompletableFuture.completedFuture(AppendEntriesResponse.newBuilder()
                            .setTerm(r.getTerm()).setSuccess(true).build());
                }

                @Override
                public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest r) {
                    return CompletableFuture.completedFuture(
                            InstallSnapshotResponse.newBuilder().setTerm(r.getTerm()).build());
                }

                @Override
                public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest r) {
                    return CompletableFuture.completedFuture(TimeoutNowResponse.newBuilder().build());
                }

                @Override
                public void close() { }
            };
        }
    }
}
