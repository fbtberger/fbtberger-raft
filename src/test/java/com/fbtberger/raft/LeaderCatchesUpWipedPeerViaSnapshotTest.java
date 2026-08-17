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
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;

/**
 * v118 — a leader must catch a wiped peer up via InstallSnapshot even when it still holds that
 * peer's stale, pre-wipe matchIndex.
 *
 * <h2>The bug this pins down</h2>
 * On a rejected AppendEntries the leader set {@code nextIndex = matchIndex + 1}. That is safe only
 * while matchIndex is truthful. A peer that is <b>wiped and recreated</b> (fresh volume, empty log)
 * violates that: the leader still holds the peer's old, high matchIndex, so nextIndex is pinned
 * above {@code snapshotIndex} and {@code replicateTo}'s "{@code nextIndex <= snapshotIndex} =>
 * InstallSnapshot" switch never fires. The peer is probed at a prevLogIndex it can never satisfy,
 * rejects forever, and stays at appliedIndex 0 — the tail that outlives the v117 transport rebuild
 * (which un-sticks the <i>channel</i>, but not the stale index behind it). On dev this stranded a
 * freshly-wiped kwatro-5 at 0 with the leader frozen at {@code match=730 next=731}; on Hetzner it
 * would strand every rebuilt data host.
 *
 * <h2>The fix</h2>
 * The follower reports its own last log index on a reject ({@code conflictLastLogIndex}); the leader
 * pulls both pointers down to it. A wiped peer reports 0, so nextIndex drops to 1, falls at/under
 * snapshotIndex, and the very next replicateTo switches to InstallSnapshot.
 *
 * <h2>The harness</h2>
 * n1 under test; n2 a permanently healthy voter (supplies the majority, so n1 stays leader, commits,
 * and compacts). n3 is healthy at first — it acks appends, so its matchIndex climbs to the leader's
 * tip — then {@link WipeablePeerFactory#wipeN3()} makes it an empty peer: it rejects any
 * prevLogIndex &gt; 0 reporting lastLogIndex 0, and records whether it is ever sent an InstallSnapshot.
 * Pre-fix, nextIndex stays pinned above snapshotIndex and n3 is never sent a snapshot — the assertion
 * times out. With the fix, InstallSnapshot arrives.
 */
class LeaderCatchesUpWipedPeerViaSnapshotTest {

    private static final Map<String, String> ADDRESS_OF = Map.of(
            "n1", "localhost:7101", "n2", "localhost:7102", "n3", "localhost:7103");

    private RaftNode node;
    private WipeablePeerFactory factory;

    @AfterEach
    void tearDown() {
        if (node != null) node.shutdown();
    }

    @Test
    void aWipedPeerWithStaleMatchIndexIsCaughtUpViaInstallSnapshot() throws Exception {
        factory = new WipeablePeerFactory();
        // Low snapshot threshold so a handful of committed entries makes the leader compact its
        // own log — a prerequisite for the InstallSnapshot path to be the only way back.
        node = new RaftNode(configFor("n1", /*threshold*/ 4), new InMemoryStorage(),
                new KeyValueStateMachine(), factory, RaftMetrics.noop());
        node.start();

        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role() == ServerRole.LEADER);

        // While n3 is healthy it acks these, so the leader's matchIndex[n3] climbs to the tip.
        for (int i = 0; i < 8; i++) {
            node.submitCommand(("SET k" + i + " " + i).getBytes(StandardCharsets.UTF_8))
                    .get(2, TimeUnit.SECONDS);
        }

        // The leader crosses the threshold and compacts: snapshotIndex advances past 0. (COW runs
        // on a background thread, so this settles asynchronously — hence the await.)
        await().atMost(5, TimeUnit.SECONDS).until(() -> node.snapshotIndex() > 0);

        // WIPE n3: empty log, high stale matchIndex still held by the leader.
        factory.wipeN3();

        // A further command keeps replication ticking; heartbeats alone would also do it.
        node.submitCommand("SET after wipe".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);

        // THE ASSERTION. Pre-fix: nextIndex pinned at matchIndex+1 (> snapshotIndex), n3 only ever
        // gets rejected heartbeats, never a snapshot — this times out. With v118: the reject hint
        // drops nextIndex to 1 (<= snapshotIndex) and InstallSnapshot is sent.
        await().atMost(5, TimeUnit.SECONDS).until(factory::n3ReceivedInstallSnapshot);
    }

    private static RaftConfig configFor(String selfId, int threshold) throws Exception {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", selfId);
        props.setProperty("node.port", "7101");
        props.setProperty("data.dir", "/tmp/raft-wiped-unused/" + selfId);
        props.setProperty("snapshot.threshold", Integer.toString(threshold));
        ADDRESS_OF.forEach((id, addr) -> props.setProperty("peer." + id, addr));

        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-wiped-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    // ── Transport doubles ─────────────────────────────────────────────────────

    /** n2 is always healthy; n3 flips from healthy to wiped and records InstallSnapshot receipt. */
    private static final class WipeablePeerFactory implements RaftTransportFactory {

        private final AtomicBoolean n3Wiped = new AtomicBoolean(false);
        private final AtomicBoolean n3GotSnapshot = new AtomicBoolean(false);

        void wipeN3() {
            n3Wiped.set(true);
        }

        boolean n3ReceivedInstallSnapshot() {
            return n3GotSnapshot.get();
        }

        @Override
        public RaftTransport connect(String address) {
            String peerId = idOf(address);
            if (peerId.equals("n3")) {
                return new WipeableStub(n3Wiped, n3GotSnapshot);
            }
            return new HealthyStub();
        }

        private static String idOf(String address) {
            return ADDRESS_OF.entrySet().stream()
                    .filter(e -> e.getValue().equals(address))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("unknown address: " + address));
        }
    }

    /** Always acks appends and grants votes — the reliable majority partner. */
    private static final class HealthyStub implements RaftTransport {
        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest r) {
            return CompletableFuture.completedFuture(AppendEntriesResponse.newBuilder()
                    .setTerm(r.getTerm()).setSuccess(true).build());
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest r) {
            return CompletableFuture.completedFuture(RequestVoteResponse.newBuilder()
                    .setTerm(r.getTerm()).setVoteGranted(true).build());
        }

        @Override
        public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest r) {
            return CompletableFuture.completedFuture(PreVoteResponse.newBuilder()
                    .setTerm(0).setVoteGranted(true).build());
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

    /**
     * Healthy until wiped. Once wiped it is an empty peer: any AppendEntries with prevLogIndex &gt; 0
     * is rejected reporting conflictLastLogIndex=0, and an InstallSnapshot is accepted and recorded.
     */
    private static final class WipeableStub implements RaftTransport {
        private final AtomicBoolean wiped;
        private final AtomicBoolean gotSnapshot;

        WipeableStub(AtomicBoolean wiped, AtomicBoolean gotSnapshot) {
            this.wiped = wiped;
            this.gotSnapshot = gotSnapshot;
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest r) {
            if (!wiped.get() || r.getPrevLogIndex() == 0) {
                return CompletableFuture.completedFuture(AppendEntriesResponse.newBuilder()
                        .setTerm(r.getTerm()).setSuccess(true).build());
            }
            // Wiped: empty log, cannot satisfy any prevLogIndex > 0. Report last index 0.
            return CompletableFuture.completedFuture(AppendEntriesResponse.newBuilder()
                    .setTerm(r.getTerm()).setSuccess(false).setConflictLastLogIndex(0).build());
        }

        @Override
        public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest r) {
            gotSnapshot.set(true);
            return CompletableFuture.completedFuture(
                    InstallSnapshotResponse.newBuilder().setTerm(r.getTerm()).build());
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest r) {
            return CompletableFuture.completedFuture(RequestVoteResponse.newBuilder()
                    .setTerm(r.getTerm()).setVoteGranted(true).build());
        }

        @Override
        public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest r) {
            return CompletableFuture.completedFuture(PreVoteResponse.newBuilder()
                    .setTerm(0).setVoteGranted(true).build());
        }

        @Override
        public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest r) {
            return CompletableFuture.completedFuture(TimeoutNowResponse.newBuilder().build());
        }

        @Override
        public void close() { }
    }
}
