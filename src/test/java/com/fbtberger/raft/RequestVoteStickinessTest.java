/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * v125 / §4.2.3 -- leader stickiness must also guard {@code RequestVote}, and must guard it by
 * <em>ignoring</em> the request rather than merely denying the vote.
 *
 * <h2>The gap</h2>
 * v122 gave {@code handlePreVote} a working stickiness check. {@code handleRequestVote} never had
 * one at all, so anything that reaches it directly -- a candidate that skips PreVote, an old peer,
 * a hostile one -- could still unseat a healthy leader.
 *
 * <h2>Why denying the vote is not enough</h2>
 * The pre-v125 handler adopted a higher term (via {@code becomeFollowerLocked}) as its very first
 * act. A guard placed after that would deny the vote and still have done the damage: adopting the
 * term deposes the leader we were happily following and forces a fresh election. The disruption
 * stickiness exists to prevent IS the term bump, not the granted vote. Hence the check runs first
 * and returns our unchanged term.
 *
 * <h2>Why the carve-out exists</h2>
 * In a leadership transfer (§3.10) the incumbent itself sends TimeoutNow to the target. Every other
 * voter is still hearing from that leader -- correctly, it is alive -- so a strict stickiness check
 * would make all of them refuse and the transfer would hang until the timeout aborts it. The
 * candidate therefore marks those RequestVotes with {@code leadershipTransfer}, and voters skip the
 * check for those only.
 */
class RequestVoteStickinessTest {

    private static final Map<String, String> ADDRESS_OF = Map.of(
            "n1", "localhost:7501", "n2", "localhost:7502", "n3", "localhost:7503");

    private RaftNode node;
    private InMemoryStorage store;
    private RecordingPeers peers;

    @BeforeEach
    void boot() {
        peers = new RecordingPeers();
        store = new InMemoryStorage();
        node = new RaftNode(configFor("n1"), store, new KeyValueStateMachine(),
                peers, RaftMetrics.noop());
        // No start(): the background election timer would race these assertions.
    }

    @AfterEach
    void tearDown() {
        if (node != null) node.shutdown();
    }

    /**
     * THE REGRESSION, first half: a voter that still hears a leader must refuse.
     */
    @Test
    void aVoterWithRecentLeaderContactDeniesTheVote() {
        node.start();   // seeds lastLeaderContactMs -- we "just heard" from a leader

        RequestVoteResponse response = node.handleRequestVote(voteRequestFrom("n2", 5, false));

        assertFalse(response.getVoteGranted(), "stickiness must protect the incumbent");
    }

    /**
     * THE REGRESSION, second half, and the part that matters more: refusing is worthless if we
     * adopt the candidate's term anyway -- that alone deposes our leader.
     */
    @Test
    void aDeniedVoteMustNotAdoptTheCandidatesTerm() {
        node.start();
        long termBefore = store.getCurrentTerm();

        RequestVoteResponse response = node.handleRequestVote(voteRequestFrom("n2", 99, false));

        assertEquals(termBefore, store.getCurrentTerm(),
                "adopting the term is the disruption -- denying the vote does not undo it");
        assertEquals(termBefore, response.getTerm(), "and we must report our own, unchanged term");
    }

    /**
     * The carve-out. Without it, adding the guard above silently breaks transferLeadership.
     */
    @Test
    void aLeadershipTransferBypassesStickiness() {
        node.start();

        RequestVoteResponse response = node.handleRequestVote(voteRequestFrom("n2", 5, true));

        assertTrue(response.getVoteGranted(),
                "a transfer is requested BY the incumbent -- voters must not block it");
    }

    /**
     * The counterweight: with no leader in sight, a voter must still grant, or a genuinely dead
     * leader could never be replaced.
     */
    @Test
    void aVoterWithoutRecentLeaderContactStillGrants() {
        // Never started, never contacted: lastLeaderContactMs is still 0.
        RequestVoteResponse response = node.handleRequestVote(voteRequestFrom("n2", 5, false));

        assertTrue(response.getVoteGranted(),
                "without leader contact a vote must be possible, else the cluster cannot recover");
    }

    /**
     * The end-to-end shape of the carve-out: a TimeoutNow from the incumbent must produce
     * RequestVotes that actually carry the flag. Without this the carve-out exists but is never
     * used, and transfers break in exactly the way the unit above pretends to rule out.
     */
    @Test
    void timeoutNowProducesRequestVotesMarkedAsTransfer() {
        node.handleTimeoutNow(TimeoutNowRequest.newBuilder().build());

        assertFalse(peers.voteRequests().isEmpty(), "precondition: an election was started");
        assertTrue(peers.voteRequests().stream().allMatch(RequestVoteRequest::getLeadershipTransfer),
                "every RequestVote from a TimeoutNow must be marked as a transfer");
    }

    /** And the converse: an ordinary election must NOT claim to be a transfer. */
    @Test
    void anOrdinaryElectionDoesNotClaimToBeATransfer() {
        node.startElection();

        assertFalse(peers.voteRequests().isEmpty(), "precondition: an election was started");
        assertTrue(peers.voteRequests().stream().noneMatch(RequestVoteRequest::getLeadershipTransfer),
                "an ordinary campaign must not set the bypass flag");
    }

    // -- Harness ---------------------------------------------------------------

    private static RequestVoteRequest voteRequestFrom(String candidate, long term, boolean transfer) {
        return RequestVoteRequest.newBuilder()
                .setCandidateId(candidate).setTerm(term)
                .setLastLogIndex(0).setLastLogTerm(0)
                .setLeadershipTransfer(transfer)
                .build();
    }

    private static RaftConfig configFor(String selfId) {
        try {
            java.util.Properties props = new java.util.Properties();
            props.setProperty("node.id", selfId);
            props.setProperty("node.port", "7501");
            props.setProperty("data.dir", "/tmp/raft-rvstickiness-unused/" + selfId);
            ADDRESS_OF.forEach((id, addr) -> props.setProperty("peer." + id, addr));
            java.nio.file.Path tmp =
                    java.nio.file.Files.createTempFile("raft-rvstickiness-", ".properties");
            try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
                props.store(out, null);
            }
            return RaftConfig.load(tmp);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Grants everything inline and records the RequestVotes that went out. */
    private static final class RecordingPeers implements RaftTransportFactory {

        private final List<RequestVoteRequest> voteRequests = new CopyOnWriteArrayList<>();

        List<RequestVoteRequest> voteRequests() {
            return voteRequests;
        }

        @Override
        public RaftTransport connect(String address) {
            return new RaftTransport() {
                @Override
                public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest r) {
                    voteRequests.add(r);
                    return CompletableFuture.completedFuture(RequestVoteResponse.newBuilder()
                            .setTerm(r.getTerm()).setVoteGranted(true).build());
                }

                @Override
                public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest r) {
                    return CompletableFuture.completedFuture(PreVoteResponse.newBuilder()
                            .setTerm(0).setVoteGranted(true).build());
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
