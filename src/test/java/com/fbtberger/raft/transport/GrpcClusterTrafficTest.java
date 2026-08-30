/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import com.fbtberger.raft.KeyValueStateMachine;
import com.fbtberger.raft.RaftConfig;
import com.fbtberger.raft.RaftMetrics;
import com.fbtberger.raft.RaftNode;
import com.fbtberger.raft.ServerRole;
import com.fbtberger.raft.proto.*;
import com.fbtberger.raft.InMemoryStorage;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A real three-node cluster over gRPC, and proof that the gRPC server actually carried it.
 *
 * <p><b>Why this exists as a test of its own.</b> {@code MultiTransportClusterTest} runs the same
 * three nodes over each transport in turn, and its gRPC case passes — yet
 * {@code GrpcTransportServer$RaftServiceAdapter.requestVote} and {@code .appendEntries} showed as
 * uncovered, all ten lines of them, and that was most of a twelve-line run-to-run swing in the
 * repository's coverage. Two things that cannot both be true: either those handlers run and the
 * measurement is wrong, or the election happens without them and the transport under test is not
 * the one being exercised.
 *
 * <p>This test decides it. It asserts on the handlers themselves rather than on a leader
 * appearing: the counters below are incremented inside the server-side handler, so a green run
 * says the gRPC path carried a vote and an append, and a red one says it did not — which would be
 * the more interesting answer.
 *
 * <p>Latches, not sleeps and not polling: there is an EVENT here to be told about. The handler is
 * the seam every incoming RPC passes through, so the test can wait for the RPC rather than for a
 * plausible number of milliseconds. That is the whole difference between a test that sometimes
 * covers a line and one that always does.
 */
class GrpcClusterTrafficTest {

    private static final int NODES = 3;
    private static final long TIMEOUT_MS = 10_000;

    private final Map<String, RaftNode> nodes = new HashMap<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private final Map<String, CountingHandler> handlers = new HashMap<>();

    @AfterEach
    void tearDown() {
        nodes.values().forEach(RaftNode::shutdown);
        for (AutoCloseable c : closeables) {
            try { c.close(); } catch (Exception ignored) { /* teardown is best effort */ }
        }
    }

    @Test
    @DisplayName("A gRPC cluster elects a leader, and the gRPC server is what carried the votes")
    void theGrpcServerCarriesVotesAndAppends() throws Exception {
        startCluster();

        RaftNode leader = awaitLeader();
        assertNotNull(leader, "a leader should have been elected");

        assertTrue(awaitAny(h -> h.votes), "no RequestVote reached a gRPC server");
        assertTrue(awaitAny(h -> h.appends), "no AppendEntries reached a gRPC server");
    }

    /**
     * And the same for the payload path: a command submitted to the leader has to travel to the
     * followers as an AppendEntries carrying entries, not merely as a heartbeat carrying none.
     * The distinction matters — a cluster can look healthy on heartbeats alone.
     */
    @Test
    @DisplayName("A committed command travels over gRPC as an AppendEntries carrying entries")
    void aCommandTravelsAsEntries() throws Exception {
        startCluster();
        RaftNode leader = awaitLeader();
        assertNotNull(leader);

        leader.submitCommand("SET k v".getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);

        assertTrue(awaitAny(h -> h.appendsWithEntries),
                "no AppendEntries carrying entries reached a gRPC server");
    }

    // ---- helpers ------------------------------------------------------------

    /** True as soon as ONE node's latch has fired; a single follower answering is the claim. */
    private boolean awaitAny(java.util.function.Function<CountingHandler, CountDownLatch> pick)
            throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT_MS * 1_000_000L;
        while (System.nanoTime() < deadline) {
            for (CountingHandler h : handlers.values()) {
                if (pick.apply(h).await(20, TimeUnit.MILLISECONDS)) return true;
            }
        }
        return false;
    }

    private RaftNode awaitLeader() throws InterruptedException {
        long deadline = System.nanoTime() + TIMEOUT_MS * 1_000_000L;
        while (System.nanoTime() < deadline) {
            RaftNode l = nodes.values().stream()
                    .filter(n -> n.role() == ServerRole.LEADER).findFirst().orElse(null);
            if (l != null) return l;
            Thread.sleep(10);
        }
        return null;
    }

    private void startCluster() throws Exception {
        Map<String, Integer> ports = new HashMap<>();
        for (int i = 1; i <= NODES; i++) ports.put("n" + i, freePort());
        Map<String, String> peers = new HashMap<>();
        ports.forEach((id, port) -> peers.put(id, "localhost:" + port));

        // Servers first: a node connects eagerly, so every peer has to be listening already.
        for (var e : ports.entrySet()) {
            CountingHandler h = new CountingHandler();
            handlers.put(e.getKey(), h);
            GrpcTransportServer server = new GrpcTransportServer(e.getValue(), h);
            server.start();
            closeables.add(server);
        }

        for (var e : ports.entrySet()) {
            RaftNode node = new RaftNode(
                    config(e.getKey(), e.getValue(), peers),
                    new InMemoryStorage(), new KeyValueStateMachine(),
                    address -> {
                        GrpcTransport t = new GrpcTransport(
                                ManagedChannelBuilder.forTarget(address).usePlaintext().build());
                        closeables.add(t);
                        return t;
                    },
                    RaftMetrics.noop());
            nodes.put(e.getKey(), node);
            handlers.get(e.getKey()).delegate = node;
        }

        nodes.values().forEach(RaftNode::start);
    }

    private static RaftConfig config(String id, int port, Map<String, String> peers)
            throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", id);
        props.setProperty("node.port", String.valueOf(port));
        props.setProperty("data.dir", "/tmp/raft-grpc-traffic-test/" + id);
        props.setProperty("snapshot.threshold", "100");
        peers.forEach((k, v) -> props.setProperty("peer." + k, v));
        Path tmp = Files.createTempFile("raft-grpc-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    /**
     * A free port, with the usual caveat: between closing this socket and the server binding, the
     * port could be taken. Nothing else in this repository does better, and a collision fails
     * loudly at bind time rather than quietly later.
     */
    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /**
     * The server-side handler, counting what actually arrived.
     *
     * <p>Three latches rather than a boolean each: a latch is what a test can WAIT on, which is
     * the point of doing it this way instead of sleeping and hoping.
     */
    private static final class CountingHandler implements RaftRpcHandler {
        volatile RaftRpcHandler delegate;
        final CountDownLatch votes = new CountDownLatch(1);
        final CountDownLatch appends = new CountDownLatch(1);
        final CountDownLatch appendsWithEntries = new CountDownLatch(1);

        @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest r) {
            votes.countDown();
            return delegate.handleRequestVote(r);
        }
        @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest r) {
            appends.countDown();
            if (r.getEntriesCount() > 0) appendsWithEntries.countDown();
            return delegate.handleAppendEntries(r);
        }
        @Override public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest r) {
            return delegate.handleInstallSnapshot(r);
        }
        @Override public PreVoteResponse handlePreVote(PreVoteRequest r) {
            return delegate.handlePreVote(r);
        }
        @Override public TimeoutNowResponse handleTimeoutNow(TimeoutNowRequest r) {
            return delegate.handleTimeoutNow(r);
        }
    }
}
