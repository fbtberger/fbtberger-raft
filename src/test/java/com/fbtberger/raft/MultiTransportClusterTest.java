/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
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
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class MultiTransportClusterTest {

    enum Transport {
        GRPC, NETTY, HADOOP
    }

    private final Map<String, RaftNode> nodes = new HashMap<>();
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
        awaitLeader(5_000);
        long leaders = nodes.values().stream().filter(n -> n.role() == ServerRole.LEADER).count();
        assertEquals(1, leaders, "exactly one leader expected");
    }

    @ParameterizedTest
    @EnumSource(Transport.class)
    void leaderReplicatesCommand(Transport transport) throws Exception {
        startCluster(transport, 3);
        awaitLeader(5_000);
        RaftNode leader = leader();
        assertNotNull(leader);

        byte[] result = leader.submitCommand("SET k v".getBytes(StandardCharsets.UTF_8))
                .get(5, TimeUnit.SECONDS);
        assertEquals("OK", new String(result, StandardCharsets.UTF_8));

        Thread.sleep(300);
        for (KeyValueStateMachine sm : machines.values()) {
            assertEquals("v", sm.get("k"), "all state machines must converge");
        }
    }

    @ParameterizedTest
    @EnumSource(Transport.class)
    void followerRejectsCommand(Transport transport) throws Exception {
        startCluster(transport, 3);
        awaitLeader(5_000);
        RaftNode follower = nodes.values().stream()
                .filter(n -> n.role() != ServerRole.LEADER).findFirst().orElseThrow();
        var f = follower.submitCommand("SET x 1".getBytes(StandardCharsets.UTF_8));
        assertTrue(f.isCompletedExceptionally());
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

    private static final class DelegatingHandler implements RaftRpcHandler {
        private volatile RaftRpcHandler delegate;
        void setDelegate(RaftRpcHandler d) { this.delegate = d; }

        @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest r) {
            return delegate.handleRequestVote(r);
        }
        @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest r) {
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

    private RaftNode leader() {
        return nodes.values().stream()
                .filter(n -> n.role() == ServerRole.LEADER).findFirst().orElse(null);
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
