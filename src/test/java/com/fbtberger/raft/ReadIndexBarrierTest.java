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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ReadIndex barrier (§6.4) and joint consensus (§4.3), against a real {@link RaftNode} whose
 * peers can be told when to acknowledge.
 *
 * <h2>Why this file exists</h2>
 * {@code RaftNodeTest} already has ReadIndex tests — and every one of them runs against a
 * <b>single-node</b> cluster, where {@code readIndex()} returns before the barrier is ever built:
 *
 * <pre>
 *   if (majority() == 1) { return ...; }        // the tested path
 *   ReadBarrier barrier = new ReadBarrier(ri);  // never reached
 * </pre>
 *
 * So the quorum confirmation, the voting-member rule, and the step-down handling had never once
 * been executed. That is how v108's bug survived: the barrier's quorum rule was wrong under joint
 * consensus, and nothing ever ran it.
 *
 * <p>Coverage that skips the mechanism is not coverage. These tests build a leader with several
 * voters and withhold acknowledgements on purpose, so the barrier actually has to decide.
 */
class ReadIndexBarrierTest {

    private static final long OK = 2_000;   // generous: real election timers are in play

    /** Real-looking addresses: RaftConfig should never have to accept an invented URI scheme. */
    private static final Map<String, String> ADDRESS_OF = Map.of(
            "n1", "localhost:9001", "n2", "localhost:9002", "n3", "localhost:9003",
            "n4", "localhost:9004", "n5", "localhost:9005", "L1", "localhost:9101");

    private final Map<String, ScriptedPeer> peers = new ConcurrentHashMap<>();
    private RaftNode node;

    private static String idOf(String address) {
        return ADDRESS_OF.entrySet().stream()
                .filter(e -> e.getValue().equals(address))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("unknown peer address: " + address));
    }

    @AfterEach
    void tearDown() {
        if (node != null) node.shutdown();
    }

    // ── the quorum confirmation ──────────────────────────────────────────────

    @Test
    void aReadIndexIsNotServedUntilAQuorumConfirmsTheLeadership() throws Exception {
        startLeader();                       // C = {n1, n2, n3}; majority = 2
        silence("n2", "n3");

        CompletableFuture<Long> read = node.readIndex();

        // Nobody has confirmed us. §6.4 exists precisely so that a deposed leader cannot answer
        // from its own stale state — so this MUST NOT complete.
        assertThrows(TimeoutException.class, () -> read.get(300, TimeUnit.MILLISECONDS),
                "a read was served without any peer confirming our leadership");

        peers.get("n2").acknowledge();       // one voter is a majority of three, with ourselves

        assertTrue(read.get(OK, TimeUnit.MILLISECONDS) >= 0);
    }

    /**
     * §4.2.1. A learner replicates the log but does not vote — so its acknowledgement says nothing
     * about whether we are still the leader. If it counted, a leader abandoned by every voter could
     * confirm its own leadership with non-voters and serve a stale read as linearizable.
     */
    @Test
    void aLearnersAcknowledgementCannotConfirmLeadership() throws Exception {
        startLeader();
        silence("n2", "n3");
        node.addLearner("L1", ADDRESS_OF.get("L1"));   // effective on append; we do not await the commit
        await().atMost(OK, TimeUnit.MILLISECONDS).until(() -> peers.containsKey("L1"));

        CompletableFuture<Long> read = node.readIndex();
        peers.get("L1").acknowledge();

        assertThrows(TimeoutException.class, () -> read.get(300, TimeUnit.MILLISECONDS),
                "a learner's ack satisfied a read barrier — it is not a vote");

        peers.get("n2").acknowledge();       // a voter, at last

        assertTrue(read.get(OK, TimeUnit.MILLISECONDS) >= 0);
    }

    /**
     * A leader that discovers a higher term is no longer the leader, and the reads it had promised
     * are no longer safe to answer. Leaving them pending — or, worse, completing them — would serve
     * a read on the authority of a leadership that no longer exists.
     */
    @Test
    void aLeaderThatStepsDownFailsTheReadsItHadNotYetServed() throws Exception {
        startLeader();
        silence("n2", "n3");

        CompletableFuture<Long> read = node.readIndex();
        assertFalse(read.isDone());

        // A new leader in a much higher term announces itself. (RaftNode exposes no currentTerm(),
        // and it does not need to: an election here reaches term 1 or 2, so 1000 is unambiguously
        // higher and the node must step down.)
        node.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(1000)
                .setLeaderId("n2")
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .setLeaderCommit(0)
                .build());

        ExecutionException thrown = assertThrows(ExecutionException.class,
                () -> read.get(OK, TimeUnit.MILLISECONDS));
        assertInstanceOf(RaftNode.NotLeaderException.class, thrown.getCause(),
                "a pending read must fail as 'not the leader', not hang and not succeed");
    }

    // ── joint consensus (§4.3) — the v108 bug, end to end ────────────────────

    /**
     * THE REGRESSION, in a running node.
     *
     * <p>C_old = {n1, n2, n3}, C_new = {n1, n4, n5}, leader = n1. Under the old rule the barrier
     * needed {@code confirmed.size() + 1 >= max(2, 2)}, so a single ack from n4 was "enough":
     *
     * <pre>
     *   C_new: {n1, n4} = 2 of 3  ->  a majority
     *   C_old: {n1}     = 1 of 3  ->  NOT a majority
     * </pre>
     *
     * The leader would have confirmed its leadership with no majority of C_old behind it, and
     * served the read as linearizable. Joint consensus asks for a majority in BOTH configurations,
     * and that is what the barrier must require.
     */
    @Test
    void duringAConfigurationChange_aMajorityOfTheNewConfigurationAloneDoesNotConfirmLeadership()
            throws Exception {
        startLeader();
        silence("n2", "n3");

        // Enter joint consensus. The entry takes effect on APPEND, so C_old,new is live at once;
        // it cannot commit while C_old stays silent, which is exactly how we hold the node here.
        node.setConfiguration(Map.of(
                "n1", ADDRESS_OF.get("n1"),
                "n4", ADDRESS_OF.get("n4"),
                "n5", ADDRESS_OF.get("n5")));
        await().atMost(OK, TimeUnit.MILLISECONDS)
                .until(() -> peers.containsKey("n4") && peers.containsKey("n5"));

        CompletableFuture<Long> read = node.readIndex();
        peers.get("n4").acknowledge();       // a majority of C_new (with ourselves) — and nothing more

        assertThrows(TimeoutException.class, () -> read.get(400, TimeUnit.MILLISECONDS),
                "C_new alone confirmed the leadership — C_old never had a say");

        peers.get("n2").acknowledge();       // now C_old has a majority too

        assertTrue(read.get(OK, TimeUnit.MILLISECONDS) >= 0,
                "with both majorities behind it, the read must be served");
    }

    // ── harness ──────────────────────────────────────────────────────────────

    /**
     * A three-voter leader, with its term's no-op already committed.
     *
     * <p>The peers acknowledge while this runs — they have to, or the no-op never commits and the
     * node may not touch its configuration. Tests call {@link #silence} afterwards to take the
     * quorum away deliberately.
     */
    private void startLeader() throws Exception {
        RaftConfig config = configFor("n1", Map.of(
                "n1", ADDRESS_OF.get("n1"),
                "n2", ADDRESS_OF.get("n2"),
                "n3", ADDRESS_OF.get("n3")));

        node = new RaftNode(config, new InMemoryStorage(), new KeyValueStateMachine(),
                this::connect, RaftMetrics.noop());
        node.start();

        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> node.role() == ServerRole.LEADER);

        // §4 errata: a leader may not change the configuration until it has committed an entry
        // from its OWN term — otherwise it cannot know what the latest committed configuration is.
        // The no-op it appends on election is what satisfies that, and it is why the peers still
        // acknowledge at this point: silencing them before the no-op commits would make every
        // addLearner()/setConfiguration() below fail with "leader has not yet committed an entry in
        // its current term". Correct behaviour on the node's part — and a rule the harness has to
        // respect rather than trip over.
        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> node.appliedIndex() >= 1);
    }

    /** Peers stop acknowledging: they stay reachable, they simply say nothing. */
    private void silence(String... ids) {
        for (String id : ids) {
            ScriptedPeer peer = peers.get(id);
            if (peer != null) peer.goSilent();
        }
    }

    private RaftTransport connect(String address) {
        return peers.computeIfAbsent(idOf(address), ScriptedPeer::new);
    }

    private static RaftConfig configFor(String selfId, Map<String, String> members) throws Exception {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", selfId);
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-readindex-unused/" + selfId);
        props.setProperty("snapshot.threshold", "100000");
        members.forEach((id, addr) -> props.setProperty("peer." + id, addr));

        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-ri-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    /**
     * A peer that always grants votes — so the node under test reliably becomes leader — but
     * acknowledges AppendEntries only when the test says so.
     *
     * <p>Withholding an acknowledgement is modelled as a future that never completes, not as a
     * failure: a silent peer must leave the leader in office (it only steps down for a higher
     * term), so the barrier is genuinely waiting on a quorum rather than reacting to an error.
     */
    private static final class ScriptedPeer implements RaftTransport {

        private final String id;
        private final AtomicBoolean acking = new AtomicBoolean(true);
        /** Acks withheld so far — completed as soon as the test lets this peer speak. */
        private final java.util.List<CompletableFuture<AppendEntriesResponse>> withheld =
                java.util.Collections.synchronizedList(new java.util.ArrayList<>());
        private volatile long lastTermSeen;

        ScriptedPeer(String id) {
            this.id = id;
        }

        void goSilent() {
            acking.set(false);
        }

        /** Let this peer answer — including the requests it is currently sitting on. */
        void acknowledge() {
            acking.set(true);
            synchronized (withheld) {
                for (CompletableFuture<AppendEntriesResponse> f : withheld) {
                    f.complete(ok(lastTermSeen));
                }
                withheld.clear();
            }
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
            lastTermSeen = request.getTerm();
            if (acking.get()) {
                return CompletableFuture.completedFuture(ok(request.getTerm()));
            }
            CompletableFuture<AppendEntriesResponse> pending = new CompletableFuture<>();
            withheld.add(pending);
            return pending;   // silence: never completes until acknowledge()
        }

        private static AppendEntriesResponse ok(long term) {
            return AppendEntriesResponse.newBuilder().setTerm(term).setSuccess(true).build();
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
            // Echo the candidate's term: equal, never greater, so the vote cannot depose it.
            return CompletableFuture.completedFuture(RequestVoteResponse.newBuilder()
                    .setTerm(request.getTerm()).setVoteGranted(true).build());
        }

        @Override
        public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest request) {
            // A pre-vote request carries currentTerm + 1. Echoing THAT back would look like a
            // higher term and send the candidate straight back to follower, so report 0.
            return CompletableFuture.completedFuture(PreVoteResponse.newBuilder()
                    .setTerm(0).setVoteGranted(true).build());
        }

        @Override
        public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest r) {
            return new CompletableFuture<>();
        }

        @Override
        public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest r) {
            return new CompletableFuture<>();
        }

        @Override
        public void close() {
            synchronized (withheld) {
                withheld.forEach(f -> f.cancel(false));
                withheld.clear();
            }
        }

        @Override
        public String toString() {
            return "ScriptedPeer(" + id + ")";
        }
    }
}
