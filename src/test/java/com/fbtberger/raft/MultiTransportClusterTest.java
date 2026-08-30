/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.*;
import com.fbtberger.raft.transport.*;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MultiTransportClusterTest {

    enum Transport {
        GRPC, NETTY, HADOOP
    }

    private final Map<String, RaftNode> nodes = new HashMap<>();
    /** The wrappers around every node's incoming RPCs, for the latches in [DelegatingHandler]. */
    private Map<String, DelegatingHandler> handlers = new HashMap<>();
    private final Map<String, KeyValueStateMachine> machines = new HashMap<>();
    private final List<AutoCloseable> closeables = new ArrayList<>();
    private NioEventLoopGroup nettyGroup;

    @AfterEach
    void tearDown() {
        for (RaftNode n : nodes.values())
            n.shutdown();
        for (AutoCloseable c : closeables) {
            try {
                c.close();
            } catch (Exception ignored) {
            }
        }
        if (nettyGroup != null)
            nettyGroup.shutdownGracefully();
    }

    @ParameterizedTest
    @EnumSource(Transport.class)
    void threeNodeClusterElectsLeader(Transport transport) throws Exception {
        startCluster(transport, 3);
        awaitReplicatingCluster(5_000);
        long leaders = nodes.values().stream().filter(n -> n.role() == ServerRole.LEADER).count();
        assertEquals(1, leaders, "exactly one leader expected");
    }

    @ParameterizedTest
    @EnumSource(Transport.class)
    void leaderReplicatesCommand(Transport transport) throws Exception {
        startCluster(transport, 3);
        awaitReplicatingCluster(5_000);
        RaftNode leader = leader();
        assertNotNull(leader);

        byte[] result = leader.submitCommand("SET k v".getBytes(StandardCharsets.UTF_8))
                .get(5, TimeUnit.SECONDS);
        assertEquals("OK", new String(result, StandardCharsets.UTF_8));

        // Wait for convergence itself, not for a guess at how long it takes. The sleep that
        // was here also decided, run by run, whether the follower's AppendEntries handler had
        // been reached before the test returned — which is how a passing suite still moved its
        // coverage by ten lines.
        Await.until("all state machines have converged on k=v", 5_000, () ->
                machines.values().stream().allMatch(sm -> "v".equals(sm.get("k"))));
    }

    @ParameterizedTest
    @EnumSource(Transport.class)
    void followerRejectsCommand(Transport transport) throws Exception {
        startCluster(transport, 3);
        awaitReplicatingCluster(5_000);
        RaftNode follower = nodes.values().stream()
                .filter(n -> n.role() != ServerRole.LEADER).findFirst().orElseThrow();
        var f = follower.submitCommand("SET x 1".getBytes(StandardCharsets.UTF_8));
        // The refusal travels back through the transport, so it is not necessarily there the
        // instant submitCommand returns. Reading the future immediately made this test pass for
        // the wrong reason on a fast machine and race on a slow one.
        Await.until("the follower has refused the command", 5_000, f::isCompletedExceptionally);
    }

    // ---- setup helpers --------------------------------------------------

    private void startCluster(Transport transport, int size) throws Exception {
        Map<String, Integer> ports = new HashMap<>();
        for (int i = 1; i <= size; i++) {
            ports.put("n" + i, findFreePort());
        }
        Map<String, String> peers = new HashMap<>();
        for (var e : ports.entrySet()) {
            peers.put(e.getKey(), "localhost:" + e.getValue());
        }
        if (transport == Transport.NETTY) {
            nettyGroup = new NioEventLoopGroup(2);
        }

        // Phase 1: start servers with delegating handlers
        Map<String, DelegatingHandler> handlers = new HashMap<>();
        this.handlers = handlers;
        for (var e : ports.entrySet()) {
            DelegatingHandler h = new DelegatingHandler();
            handlers.put(e.getKey(), h);
            RaftTransportServer server = switch (transport) {
                case GRPC -> new GrpcTransportServer(e.getValue(), h);
                case NETTY -> new NettyTransportServer(e.getValue(), h);
                case HADOOP -> new HadoopTransportServer(e.getValue(), h, new Configuration());
            };
            server.start();
            closeables.add(server);
        }

        // Phase 2: create nodes (connects eagerly — servers already listening)
        for (var e : ports.entrySet()) {
            String id = e.getKey();
            InMemoryStorage store = new InMemoryStorage();
            KeyValueStateMachine sm = new KeyValueStateMachine();
            machines.put(id, sm);
            RaftConfig cfg = config(id, e.getValue(), peers);
            RaftNode node = new RaftNode(cfg, store, sm, createFactory(transport), RaftMetrics.noop());
            nodes.put(id, node);
            handlers.get(id).setDelegate(node);
        }

        // Phase 3: start Raft
        for (RaftNode n : nodes.values()) n.start();
    }

    private RaftTransportFactory createFactory(Transport transport) {
        return switch (transport) {
            case GRPC -> address -> {
                GrpcTransport t = new GrpcTransport(
                        io.grpc.ManagedChannelBuilder.forTarget(address).usePlaintext().build());
                closeables.add(t);
                return t;
            };
            case NETTY -> address -> {
                String[] parts = address.split(":");
                NettyTransport t = new NettyTransport(
                        parts[0], Integer.parseInt(parts[1]), nettyGroup, null);
                closeables.add(t);
                return t;
            };
            case HADOOP -> {
                HadoopTransportFactory htf = new HadoopTransportFactory(new Configuration());
                closeables.add(htf);
                yield address -> {
                    RaftTransport t = htf.connect(address);
                    closeables.add(t);
                    return t;
                };
            }
        };
    }

    /**
     * The seam every incoming RPC passes through — and therefore the place to be told that one
     * has arrived, rather than to guess that it has by now.
     *
     * <p>The two latches are why they are here. Waiting for a leader is waiting for a proxy: a
     * role flips as soon as the votes are counted, while the FOLLOWERS' side of the conversation
     * — the handler that answered the vote, the handler that took the first AppendEntries — may
     * not have been reached yet when the test returns. Measured on 2026-08-30 over identical
     * suites (344 tests, no failures, no skips): those ten lines of the gRPC adapter were covered
     * in some runs and not in others, all-or-nothing, and moved the repository's line coverage by
     * twelve lines from run to run. A ratchet cannot be set against that.
     *
     * <p>A latch and not a poll, because there IS an event to be told about: this class already
     * wraps the handler. Polling is for conditions that are only readable as state — see
     * {@link Await}, which the rest of this test uses for exactly those.
     */
    private static final class DelegatingHandler implements RaftRpcHandler {
        private volatile RaftRpcHandler delegate;
        final CountDownLatch votesAnswered = new CountDownLatch(1);
        final CountDownLatch appendsAnswered = new CountDownLatch(1);

        void setDelegate(RaftRpcHandler d) { this.delegate = d; }

        @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest r) {
            try { return delegate.handleRequestVote(r); } finally { votesAnswered.countDown(); }
        }
        @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest r) {
            try { return delegate.handleAppendEntries(r); } finally { appendsAnswered.countDown(); }
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

    private RaftNode leader() {
        return nodes.values().stream()
                .filter(n -> n.role() == ServerRole.LEADER).findFirst().orElse(null);
    }

    /**
     * Wait until the cluster is not merely led but actually TALKING: a leader exists, some node
     * has answered a vote, and some node has taken an AppendEntries.
     *
     * <p>That is a stronger claim than "a leader exists", and it is the one every test here
     * actually depends on. It is also what makes the run reproducible — see [DelegatingHandler].
     */
    private void awaitReplicatingCluster(long timeoutMs) throws InterruptedException {
        awaitLeader(timeoutMs);
        assertTrue(anyLatch(timeoutMs, h -> h.votesAnswered),
                "no node answered a RequestVote within " + timeoutMs + " ms");
        assertTrue(anyLatch(timeoutMs, h -> h.appendsAnswered),
                "no node answered an AppendEntries within " + timeoutMs + " ms");
    }

    /** True as soon as ONE of the nodes' latches has fired; a follower is enough. */
    private boolean anyLatch(long timeoutMs,
                             java.util.function.Function<DelegatingHandler, CountDownLatch> pick)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            for (DelegatingHandler h : handlers.values()) {
                if (pick.apply(h).await(20, TimeUnit.MILLISECONDS)) return true;
            }
        }
        return false;
    }

    private void awaitLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (leader() != null)
                return;
            Thread.sleep(30);
        }
        fail("No leader elected within " + timeoutMs + " ms");
    }

    private static int findFreePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static RaftConfig config(String id, int port, Map<String, String> peers) throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", id);
        props.setProperty("node.port", String.valueOf(port));
        props.setProperty("data.dir", "/tmp/raft-multi-test/" + id);
        props.setProperty("snapshot.threshold", "100");
        for (var e : peers.entrySet()) {
            props.setProperty("peer." + e.getKey(), e.getValue());
        }
        Path tmp = Files.createTempFile("raft-mt-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }
}
