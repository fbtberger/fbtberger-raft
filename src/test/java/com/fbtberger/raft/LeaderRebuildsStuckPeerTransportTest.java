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
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * v117 — a leader whose transport to a peer has gone into a persistent io-failure must discard
 * that transport and build a fresh one, instead of hammering the dead channel forever.
 *
 * <h2>The bug this pins down</h2>
 * The peer transport is cached in {@code peerTransports} and reused on every {@code replicateTo}.
 * A plain gRPC channel that has failed against a peer which was <b>recreated</b> (new container,
 * new IP) does not always re-resolve DNS and reconnect on its own — it can keep returning
 * {@code UNAVAILABLE} indefinitely while the leader's {@code matchIndex} for that peer stays
 * frozen and the peer never receives another entry. That is the "learner stuck at 0, leader
 * silent" signature: seen in production when a single data node was recreated under a leader that
 * already held a poisoned channel to it.
 *
 * <p>The fix counts consecutive transport failures per peer and, after a few in a row, rebuilds
 * the transport via the factory. A new channel re-resolves and reconnects — which is what
 * actually un-sticks the peer.
 *
 * <h2>The harness</h2>
 * A three-node cluster {n1,n2,n3}. n1 is the node under test; n2 is a healthy voter (it supplies
 * the majority, so n1 stays leader and commits). n3's <b>first</b> transport is broken — every RPC
 * fails with UNAVAILABLE. Without the fix, n3's transport is never rebuilt: {@code connect("n3")}
 * is called exactly once and n3 never receives an entry. With the fix, after a few failures the
 * leader rebuilds it ({@code connect("n3")} a second time), and the fresh (healthy) transport
 * carries entries again.
 */
class LeaderRebuildsStuckPeerTransportTest {

    private static final Map<String, String> ADDRESS_OF = Map.of(
            "n1", "localhost:7001", "n2", "localhost:7002", "n3", "localhost:7003");

    private RaftNode node;
    private RebuildTrackingFactory factory;

    @AfterEach
    void tearDown() {
        if (node != null) node.shutdown();
    }

    @Test
    void aStuckPeerTransportIsRebuiltAndReplicationResumes() throws Exception {
        factory = new RebuildTrackingFactory();
        node = new RaftNode(configFor("n1"), new InMemoryStorage(),
                new KeyValueStateMachine(), factory, RaftMetrics.noop());
        node.start();

        await().atMost(5, TimeUnit.SECONDS).until(() -> node.role() == ServerRole.LEADER);

        // Something to replicate to n3 (the leader's own no-op already qualifies; these make it
        // concrete). They commit via n1+n2, so the leader is unaffected by n3 being down.
        node.submitCommand("SET a 1".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        node.submitCommand("SET b 2".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);

        // THE ASSERTION. n3's first channel is dead. Without the rebuild the leader keeps using it
        // forever and connect("n3") stays at 1 — this times out. With the fix it is rebuilt.
        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> factory.connectCount("n3") >= 2);

        // And the rebuilt (healthy) transport actually carries entries again — the peer is
        // un-stuck, not merely reconnected.
        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> factory.healthyTransportReceivedAppend("n3"));
    }

    private static RaftConfig configFor(String selfId) throws Exception {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", selfId);
        props.setProperty("node.port", "7001");
        props.setProperty("data.dir", "/tmp/raft-rebuild-unused/" + selfId);
        props.setProperty("snapshot.threshold", "100000");
        ADDRESS_OF.forEach((id, addr) -> props.setProperty("peer." + id, addr));

        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-rebuild-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    // ── Transport doubles ─────────────────────────────────────────────────────

    /**
     * Counts how often each peer's transport is (re)built, and hands out a broken transport for
     * n3's FIRST build and a healthy one thereafter. n2 is always healthy (the majority).
     */
    private static final class RebuildTrackingFactory implements RaftTransportFactory {

        private final Map<String, Integer> connects = new ConcurrentHashMap<>();
        private final Map<String, Boolean> healthyGotAppend = new ConcurrentHashMap<>();

        int connectCount(String peerId) {
            return connects.getOrDefault(peerId, 0);
        }

        boolean healthyTransportReceivedAppend(String peerId) {
            return healthyGotAppend.getOrDefault(peerId, false);
        }

        @Override
        public RaftTransport connect(String address) {
            String peerId = idOf(address);
            int build = connects.merge(peerId, 1, Integer::sum);
            // n3's first channel is dead; its rebuilt one (and n2 always) is healthy.
            boolean healthy = !peerId.equals("n3") || build >= 2;
            return new StubTransport(peerId, healthy, healthyGotAppend);
        }

        private static String idOf(String address) {
            return ADDRESS_OF.entrySet().stream()
                    .filter(e -> e.getValue().equals(address))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("unknown address: " + address));
        }
    }

    /** Healthy: grants votes and acks appends. Broken: every RPC fails with UNAVAILABLE. */
    private static final class StubTransport implements RaftTransport {

        private final String peerId;
        private final boolean healthy;
        private final Map<String, Boolean> healthyGotAppend;

        StubTransport(String peerId, boolean healthy, Map<String, Boolean> healthyGotAppend) {
            this.peerId = peerId;
            this.healthy = healthy;
            this.healthyGotAppend = healthyGotAppend;
        }

        private static <T> CompletableFuture<T> dead() {
            CompletableFuture<T> f = new CompletableFuture<>();
            f.completeExceptionally(Status.UNAVAILABLE.withDescription("io exception").asRuntimeException());
            return f;
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
            if (!healthy) return dead();
            healthyGotAppend.put(peerId, true);
            return CompletableFuture.completedFuture(AppendEntriesResponse.newBuilder()
                    .setTerm(request.getTerm()).setSuccess(true).build());
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
            if (!healthy) return dead();
            return CompletableFuture.completedFuture(RequestVoteResponse.newBuilder()
                    .setTerm(request.getTerm()).setVoteGranted(true).build());
        }

        @Override
        public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest request) {
            if (!healthy) return dead();
            // Report term 0 so the echo cannot look like a higher term and depose the candidate.
            return CompletableFuture.completedFuture(PreVoteResponse.newBuilder()
                    .setTerm(0).setVoteGranted(true).build());
        }

        @Override
        public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest r) {
            if (!healthy) return dead();
            return CompletableFuture.completedFuture(
                    InstallSnapshotResponse.newBuilder().setTerm(r.getTerm()).build());
        }

        @Override
        public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest r) {
            return dead();
        }

        @Override
        public void close() { }
    }
}
