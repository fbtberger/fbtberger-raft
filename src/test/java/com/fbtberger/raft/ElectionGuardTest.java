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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * v119 -- a node must run exactly one election per legitimate trigger.
 *
 * <h2>The bug this pins down (issue B: "two full elections in 52 ms")</h2>
 * A node was observed winning term 145 and then, 35 ms later, running a second complete
 * PreVote round and winning term 146. Two independent holes in the election path allow that
 * shape:
 *
 * <ol>
 *   <li><b>Stale timer tasks.</b> {@code ScheduledFuture.cancel(false)} does not stop a task
 *       that has already begun running, and a scheduled {@code startElection()} sitting on
 *       {@code lock.lock()} <i>has</i> begun. Every cancel in the election path is therefore
 *       best-effort only. Pre-fix {@code startElection()} re-validated nothing after acquiring
 *       the lock, so such a task went ahead regardless of what had happened while it waited --
 *       including the node having become LEADER, or having just heard from a live one.</li>
 *   <li><b>No role guard.</b> Neither {@code startElection()} nor {@code startRealElection()}
 *       checked the current role, so a sitting leader would increment its own term and
 *       campaign against itself.</li>
 * </ol>
 *
 * <p>Neither is free. A term bump on a freshly elected leader discards that leader's own no-op
 * and every client entry accepted since -- all still uncommitted, all silently lost.
 *
 * <h2>Why these tests drive the seam, not the scheduler</h2>
 * The window is "task started, node became leader, task then acquires the lock". The scheduler
 * is node-internal and the real timeout is 150-300 ms, so reproducing that ordering through the
 * public API would mean racing the node and hoping -- flaky by construction, which is not a
 * test. These tests drive {@code startElection()} / {@code startElectionIfCurrent()} directly
 * instead: same code path, same guards, no threads, no sleeps, no awaits.
 *
 * <p>{@link RaftNode#start()} is deliberately <b>not</b> called: {@code start()} is exactly what
 * arms the background election timer, and an armed timer would fire into these assertions.
 * Everything under test here is reachable without it.
 */
class ElectionGuardTest {

    private static final Map<String, String> ADDRESS_OF = Map.of(
            "n1", "localhost:7201", "n2", "localhost:7202", "n3", "localhost:7203");

    private RaftNode node;
    private InMemoryStorage store;
    private RecordingPeerFactory peers;

    @BeforeEach
    void boot() {
        peers = new RecordingPeerFactory();
        store = new InMemoryStorage();
        node = new RaftNode(configFor("n1"), store, new KeyValueStateMachine(),
                peers, RaftMetrics.noop());
        // No start(): see class javadoc. The node is a FOLLOWER at term 0 with no timer armed.
    }

    @AfterEach
    void tearDown() {
        if (node != null) node.shutdown();
    }

    /**
     * THE REGRESSION. A leader that runs a stale election task bumps its own term and campaigns
     * again -- exactly the 145 -&gt; 146 pair in the issue.
     *
     * <p>Pre-fix: the term advances and a second RequestVote round goes out. With v119,
     * {@code startElection()} returns immediately on {@code role == LEADER}.
     */
    @Test
    void aLeaderThatRunsAStaleElectionTaskDoesNotBumpItsOwnTerm() {
        node.startElection();
        assertEquals(ServerRole.LEADER, node.role(), "precondition: the node should have won");
        long termAsLeader = store.getCurrentTerm();
        List<Long> termsCampaignedFor = distinctTerms(peers.voteRpcTerms());

        // A timer task that began before we won, and only now gets the lock.
        node.startElection();

        assertEquals(termAsLeader, store.getCurrentTerm(),
                "a leader must not increment its own term");
        assertEquals(ServerRole.LEADER, node.role(), "and must not leave the leadership");
        assertEquals(termsCampaignedFor, distinctTerms(peers.voteRpcTerms()),
                "no second election may be started");
    }

    /**
     * The epoch guard itself: a task scheduled under a timer that has since been superseded must
     * abort before it looks at anything else, and must not open a PreVote round.
     */
    @Test
    void anElectionTaskFromASupersededTimerIsDropped() {
        long staleEpoch = node.electionEpoch() - 1;
        long termBefore = store.getCurrentTerm();

        node.startElectionIfCurrent(staleEpoch);

        assertEquals(termBefore, store.getCurrentTerm(), "a stale task must not bump the term");
        assertTrue(peers.preVoteRpcTerms().isEmpty(),
                "a stale task must not even open a PreVote round: " + peers.preVoteRpcTerms());
    }

    /**
     * The converse, so the guard above cannot be satisfied by simply never electing anyone.
     */
    @Test
    void anElectionTaskFromTheCurrentTimerRunsNormally() {
        long termBefore = store.getCurrentTerm();

        node.startElectionIfCurrent(node.electionEpoch());

        assertTrue(store.getCurrentTerm() > termBefore,
                "a current task must still be able to win an election");
        assertEquals(ServerRole.LEADER, node.role());
        // One round, not one RPC: the round fans out to both peers, so the raw list holds two
        // entries carrying the same term. Rounds are distinct terms.
        assertEquals(List.of(termBefore + 1), distinctTerms(peers.preVoteRpcTerms()),
                "exactly one PreVote round, for exactly the next term");
    }

    /**
     * Winning an election must move the epoch, otherwise the guard in the first test has nothing
     * to catch: a task scheduled while we were still a candidate would still count as current.
     */
    @Test
    void winningAnElectionInvalidatesTasksScheduledBeforeTheWin() {
        long epochAsFollower = node.electionEpoch();

        node.startElection();

        assertEquals(ServerRole.LEADER, node.role());
        assertTrue(node.electionEpoch() > epochAsFollower,
                "becoming leader must supersede any election task already in flight");
    }

    // -- Harness ---------------------------------------------------------------

    /**
     * Collapses a per-RPC recording into the rounds it represents: one election round fans out
     * to every peer, so it contributes one entry per peer, all carrying the same term.
     */
    private static List<Long> distinctTerms(List<Long> rpcTerms) {
        return rpcTerms.stream().distinct().sorted().toList();
    }

    private static RaftConfig configFor(String selfId) {
        try {
            java.util.Properties props = new java.util.Properties();
            props.setProperty("node.id", selfId);
            props.setProperty("node.port", "7201");
            props.setProperty("data.dir", "/tmp/raft-electionguard-unused/" + selfId);
            ADDRESS_OF.forEach((id, addr) -> props.setProperty("peer." + id, addr));
            java.nio.file.Path tmp =
                    java.nio.file.Files.createTempFile("raft-electionguard-", ".properties");
            try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
                props.store(out, null);
            }
            return RaftConfig.load(tmp);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Peers that grant everything immediately, and record which terms were campaigned for.
     * Immediate completion keeps the whole election inline on the calling thread -- which is
     * what makes these tests deterministic.
     */
    private static final class RecordingPeerFactory implements RaftTransportFactory {

        private final List<Long> preVoteRounds = new CopyOnWriteArrayList<>();
        private final List<Long> voteRounds = new CopyOnWriteArrayList<>();

        /**
         * One entry per PreVote RPC actually sent -- so a single round over two peers appears
         * here twice. v119's first cut asserted on the size of this list as if it counted
         * rounds; use {@link ElectionGuardTest#distinctTerms} to talk about rounds.
         */
        List<Long> preVoteRpcTerms() {
            return preVoteRounds;
        }

        /** One entry per RequestVote RPC actually sent -- see {@link #preVoteRpcTerms()}. */
        List<Long> voteRpcTerms() {
            return voteRounds;
        }

        @Override
        public RaftTransport connect(String address) {
            return new GrantingStub(this);
        }
    }

    private static final class GrantingStub implements RaftTransport {
        private final RecordingPeerFactory owner;

        GrantingStub(RecordingPeerFactory owner) {
            this.owner = owner;
        }

        @Override
        public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest r) {
            owner.preVoteRounds.add(r.getTerm());
            return CompletableFuture.completedFuture(PreVoteResponse.newBuilder()
                    .setTerm(0).setVoteGranted(true).build());
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest r) {
            owner.voteRounds.add(r.getTerm());
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
    }
}
