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
import com.fbtberger.raft.transport.RaftTransport;
import com.fbtberger.raft.transport.RaftTransportFactory;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The three election guards that were once switchable, each pinned in its fixed direction.
 *
 * <p>Until 2026-11-16 this class asserted both directions: the fix with the switch at its
 * default, and the historic defect with the switch armed, because a talk needed the bug to
 * actually appear in front of a room. The switches are gone and so are those three assertions.
 * What survives is the half that still has something to protect -- and it has to survive
 * somewhere, because deleting a defect switch is an easy way to delete the only test of the fix
 * it was the opposite of.
 *
 * <p>What is lost with the armed direction is worth writing down: a test that only pins the fix
 * cannot tell a working guard from a guard that was never reachable. Each of these three was
 * observed failing before it was fixed -- issue #3 as two full elections 52 ms apart, issue #2
 * as a restart that took the leadership off a healthy leader -- so the guards were reachable
 * when they were built. Nothing here re-establishes that.
 *
 * <p>Same harness as {@link ElectionGuardTest} and {@link PreVoteStickinessTest}: peers that
 * answer inline, so a whole election runs on the calling thread with no sleeps and no races.
 */
class ElectionFixesTest {

    private static final Map<String, String> ADDRESS_OF = Map.of(
            "n1", "localhost:7401", "n2", "localhost:7402", "n3", "localhost:7403");

    private static final int ELECTION_TIMEOUT_MIN_MS = 150;
    private static final int ELECTION_TIMEOUT_MAX_MS = 300;

    private RaftNode node;
    private InMemoryStorage store;
    private RecordingPeers peers;

    @AfterEach
    void tearDown() {
        if (node != null) node.shutdown();
    }

    // -- the PreVote quorum latch (issue #3) -----------------------------------

    /**
     * Two peers grant the same PreVote round. Latched, the second grant changes nothing: the
     * round was already converted into an election by the first.
     */
    @Test
    void aDecidedRoundElectsOnceEvenWhenEveryPeerGrants() {
        boot();

        node.startElection();

        assertEquals(ServerRole.LEADER, node.role());
        assertEquals(1, store.getCurrentTerm(), "one round, one term");
        assertEquals(List.of(1L), distinctTerms(peers.voteRpcTerms()),
                "exactly one election was campaigned for");
    }

    // -- the boot grace (issue #2, requester side) ------------------------------

    /**
     * A node that has just come up waits out several ordinary timeouts before campaigning, so a
     * leader whose transport is still reconnecting to it gets its heartbeat in first. The
     * measured gap to beat was 498-802 ms.
     */
    @Test
    void aFreshNodeArmsAnExtendedFirstTimer() {
        boot();

        node.start();

        assertTrue(node.lastArmedElectionTimeoutMs >= 6 * ELECTION_TIMEOUT_MIN_MS
                        && node.lastArmedElectionTimeoutMs <= 6 * ELECTION_TIMEOUT_MAX_MS,
                "expected a 6x boot grace, was " + node.lastArmedElectionTimeoutMs + " ms");
    }

    /**
     * The grace is a boot grace, not a permanent slowdown: the first thing that resets the timer
     * puts the node back on the normal schedule, or every real failover would inherit the delay.
     */
    @Test
    void theBootGraceAppliesToTheFirstTimerOnly() {
        boot();
        node.start();
        int bootTimeout = node.lastArmedElectionTimeoutMs;

        node.handleAppendEntries(heartbeatFrom("n2", 1));

        assertTrue(node.lastArmedElectionTimeoutMs >= ELECTION_TIMEOUT_MIN_MS
                        && node.lastArmedElectionTimeoutMs <= ELECTION_TIMEOUT_MAX_MS,
                "after leader contact the node must be on the ordinary schedule, was "
                        + node.lastArmedElectionTimeoutMs + " ms (boot timer was "
                        + bootTimeout + " ms)");
    }

    // -- leader stickiness (issue #2, responder side) ---------------------------

    /**
     * The half of issue #2 that decides the outcome: with three voters the challenger needs one
     * foreign grant, and the incumbent must not be the one who supplies it.
     */
    @Test
    void aLeaderDeniesTheVoteThatWouldUnseatIt() {
        boot();
        node.startElection();
        assertEquals(ServerRole.LEADER, node.role(), "precondition: the node should have won");

        PreVoteResponse response = node.handlePreVote(upToDatePreVoteFrom("n2"));

        assertFalse(response.getVoteGranted());
    }

    /**
     * The stickiness line is about the LEADER case only. A follower that is being heartbeated
     * refuses on the timestamp instead, and that path has to keep working on its own -- it is
     * what denies a challenger on the other four nodes of a five-node cluster.
     */
    @Test
    void aHeartbeatedFollowerRefusesOnTheTimestamp() {
        boot();
        node.handleAppendEntries(heartbeatFrom("n3", 1));

        PreVoteResponse response = node.handlePreVote(upToDatePreVoteFrom("n2"));

        assertFalse(response.getVoteGranted(),
                "a follower in contact with a leader denies on the timestamp");
    }

    // -- Harness ---------------------------------------------------------------

    private void boot() {
        peers = new RecordingPeers();
        store = new InMemoryStorage();
        node = new RaftNode(configFor("n1"), store,
                new KeyValueStateMachine(), peers, RaftMetrics.noop());
    }

    /** A challenger whose log is exactly as current as ours, so only stickiness can deny it. */
    private PreVoteRequest upToDatePreVoteFrom(String candidate) {
        return PreVoteRequest.newBuilder()
                .setCandidateId(candidate)
                .setTerm(store.getCurrentTerm() + 1)
                .setLastLogIndex(store.getLastLogIndex())
                .setLastLogTerm(store.getLastLogTerm())
                .build();
    }

    private static AppendEntriesRequest heartbeatFrom(String leaderId, long term) {
        return AppendEntriesRequest.newBuilder()
                .setTerm(term).setLeaderId(leaderId)
                .setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0)
                .build();
    }

    /** One election round fans out to every peer; rounds are distinct terms. */
    private static List<Long> distinctTerms(List<Long> rpcTerms) {
        return rpcTerms.stream().distinct().sorted().toList();
    }

    private static RaftConfig configFor(String selfId) {
        try {
            java.util.Properties props = new java.util.Properties();
            props.setProperty("node.id", selfId);
            props.setProperty("node.port", "7401");
            props.setProperty("data.dir", "/tmp/raft-electionfixes-unused/" + selfId);
            ADDRESS_OF.forEach((id, addr) -> props.setProperty("peer." + id, addr));
            java.nio.file.Path tmp =
                    java.nio.file.Files.createTempFile("raft-defectswitch-", ".properties");
            try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
                props.store(out, null);
            }
            return RaftConfig.load(tmp);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Grants everything inline and records which terms were campaigned for. */
    private static final class RecordingPeers implements RaftTransportFactory {

        private final List<Long> voteRounds = new CopyOnWriteArrayList<>();

        /** One entry per RequestVote RPC sent -- see {@link #distinctTerms}. */
        List<Long> voteRpcTerms() {
            return voteRounds;
        }

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
                    voteRounds.add(r.getTerm());
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
