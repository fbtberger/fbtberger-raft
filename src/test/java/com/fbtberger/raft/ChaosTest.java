/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.*;
import com.fbtberger.raft.transport.GrpcTransport;
import com.fbtberger.raft.transport.GrpcTransportServer;
import com.fbtberger.raft.transport.RaftTransport;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.OutputStream;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.*;
import static org.junit.jupiter.api.Assertions.*;

class ChaosTest {

    @TempDir File tempDir;

    private final Map<String, RaftNode> nodes = new HashMap<>();
    private final Map<String, KeyValueStateMachine> machines = new HashMap<>();
    private final Map<String, Map<String, PartitionableTransport>> peerTransports = new HashMap<>();
    // v117: partition / loss state lives per DIRECTED EDGE (fromNode -> toNode), independently of
    // the transport instance. The leader may now rebuild a peer's transport on persistent failure
    // (RaftNode v117); a partition must survive that — a fresh channel over a partitioned link is
    // still partitioned. Storing the state on the disposable transport made a rebuild silently heal
    // the partition. Keyed [fromNode][toNode], created once, reused by every rebuilt transport.
    private final Map<String, Map<String, LinkState>> linkStates = new java.util.concurrent.ConcurrentHashMap<>();
    private final List<Server> servers = new ArrayList<>();
    // v117: rebuilds create channels at runtime from multiple node threads, so this must be
    // thread-safe (was a plain ArrayList, only written single-threaded at startup before).
    private final List<ManagedChannel> channels = new java.util.concurrent.CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() {
        nodes.values().forEach(RaftNode::shutdown);
        channels.forEach(ManagedChannel::shutdownNow);
        servers.forEach(Server::shutdownNow);
    }

    @Test
    void majorityMakesProgressDuringMinorityPartition() throws Exception {
        startCluster(3);
        awaitLeader();
        String leaderId = leaderId();
        RaftNode leader = nodes.get(leaderId);

        String isolated = otherNode(leaderId);
        partition(isolated);

        byte[] result = leader.submitCommand("SET k1 v1".getBytes(StandardCharsets.UTF_8))
                .get(5, TimeUnit.SECONDS);
        assertEquals("OK", new String(result, StandardCharsets.UTF_8));
    }

    @Test
    void preVotePreventsTermInflationOnRejoin() throws Exception {
        startCluster(3);
        awaitLeader();

        String isolated = otherNode(leaderId());
        partition(isolated);
        Thread.sleep(600);
        heal(isolated);

        await().atMost(5, TimeUnit.SECONDS).until(() -> leaderId() != null);

        long leaderCount = nodes.values().stream()
                .filter(n -> n.role() == ServerRole.LEADER).count();
        assertEquals(1, leaderCount, "exactly one leader after partition heals");
    }

    @Test
    void isolatedNodeCatchesUpAfterPartitionHeals() throws Exception {
        startCluster(3);
        awaitLeader();

        String leaderId = leaderId();
        RaftNode leader = nodes.get(leaderId);
        String isolated = otherNode(leaderId);

        partition(isolated);
        leader.submitCommand("SET k2 v2".getBytes(StandardCharsets.UTF_8))
                .get(5, TimeUnit.SECONDS);
        heal(isolated);

        await().atMost(5, TimeUnit.SECONDS).until(
                () -> "v2".equals(machines.get(isolated).get("k2")));
    }

    @Test
    void clusterConvergesUnderPacketLoss() throws Exception {
        startCluster(3);
        awaitLeader();
        String leaderId = leaderId();
        RaftNode leader = nodes.get(leaderId);

        String lossy = otherNode(leaderId);
        setPacketLoss(lossy, 0.5);

        byte[] result = leader.submitCommand("SET k3 v3".getBytes(StandardCharsets.UTF_8))
                .get(10, TimeUnit.SECONDS);
        assertEquals("OK", new String(result, StandardCharsets.UTF_8));

        setPacketLoss(lossy, 0.0);
        await().atMost(5, TimeUnit.SECONDS).until(
                () -> "v3".equals(machines.get(lossy).get("k3")));
    }

    @Test
    void leaderPartitionedFromMajorityTriggersNewElection() throws Exception {
        startCluster(3);
        awaitLeader();

        String oldLeader = leaderId();
        partition(oldLeader);

        await().atMost(5, TimeUnit.SECONDS).until(() ->
                nodes.entrySet().stream()
                        .filter(e -> !e.getKey().equals(oldLeader))
                        .anyMatch(e -> e.getValue().role() == ServerRole.LEADER));
    }

    // ---- cluster setup ----------------------------------------------------

    private void startCluster(int size) throws Exception {
        List<String> ids = new ArrayList<>();
        for (int i = 1; i <= size; i++) ids.add("n" + i);
        for (String id : ids) peerTransports.put(id, new HashMap<>());

        Map<String, String> peers = new HashMap<>();
        for (String id : ids) peers.put(id, "chaos-" + id);

        for (String id : ids) {
            String address = "chaos-" + id;
            InMemoryStorage store = new InMemoryStorage();
            KeyValueStateMachine sm = new KeyValueStateMachine();
            machines.put(id, sm);

            RaftConfig cfg = buildConfig(id, peers);
            String nodeId = id;
            RaftNode node = new RaftNode(cfg, store, sm, peerAddress -> {
                ManagedChannel ch = InProcessChannelBuilder.forName(peerAddress)
                        .directExecutor().build();
                channels.add(ch);
                GrpcTransport base = new GrpcTransport(ch);
                String peerId = ids.stream()
                        .filter(pid -> ("chaos-" + pid).equals(peerAddress))
                        .findFirst().orElseThrow();
                // Reuse the persistent per-edge state so a rebuilt transport inherits any partition
                // / loss already in effect (v117).
                LinkState link = linkStates
                        .computeIfAbsent(nodeId, k -> new java.util.concurrent.ConcurrentHashMap<>())
                        .computeIfAbsent(peerId, k -> new LinkState());
                PartitionableTransport pt = new PartitionableTransport(base, link);
                peerTransports.get(nodeId).put(peerId, pt);
                return pt;
            }, RaftMetrics.noop());
            nodes.put(id, node);

            Server srv = InProcessServerBuilder.forName(address)
                    .addService(new GrpcTransportServer.RaftServiceAdapter(node))
                    .build().start();
            servers.add(srv);
        }

        nodes.values().forEach(RaftNode::start);
    }

    private void partition(String nodeId) {
        forEachLinkTouching(nodeId, ls -> ls.partitioned.set(true));
    }

    private void heal(String nodeId) {
        forEachLinkTouching(nodeId, ls -> { ls.partitioned.set(false); ls.lossRate = 0.0; });
    }

    /** Applies {@code op} to every directed edge into or out of {@code nodeId} (both directions). */
    private void forEachLinkTouching(String nodeId, java.util.function.Consumer<LinkState> op) {
        for (var from : linkStates.entrySet()) {
            if (from.getKey().equals(nodeId)) {
                from.getValue().values().forEach(op);            // outgoing from nodeId
            } else {
                LinkState ls = from.getValue().get(nodeId);      // incoming to nodeId
                if (ls != null) op.accept(ls);
            }
        }
    }

    private String leaderId() {
        return nodes.entrySet().stream()
                .filter(e -> e.getValue().role() == ServerRole.LEADER)
                .map(Map.Entry::getKey).findFirst().orElse(null);
    }

    private void setPacketLoss(String nodeId, double rate) {
        forEachLinkTouching(nodeId, ls -> ls.lossRate = rate);
    }

    private String otherNode(String excludeId) {
        return nodes.keySet().stream().filter(id -> !id.equals(excludeId)).findFirst().orElseThrow();
    }

    private void awaitLeader() {
        await().atMost(5, TimeUnit.SECONDS).until(() -> leaderId() != null);
        await().atMost(5, TimeUnit.SECONDS).ignoreExceptions().until(() -> {
            nodes.get(leaderId()).submitCommand(new byte[0]).get(2, TimeUnit.SECONDS);
            return true;
        });
    }

    private RaftConfig buildConfig(String id, Map<String, String> peers) throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", id);
        props.setProperty("node.port", "0");
        props.setProperty("data.dir", new File(tempDir, id).getAbsolutePath());
        props.setProperty("snapshot.threshold", "100");
        for (var e : peers.entrySet()) {
            props.setProperty("peer." + e.getKey(), e.getValue());
        }
        Path tmp = Files.createTempFile("chaos-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) { props.store(out, null); }
        return RaftConfig.load(tmp);
    }

    /** Per directed edge (fromNode -> toNode); shared across every transport rebuilt for that edge. */
    static final class LinkState {
        final AtomicBoolean partitioned = new AtomicBoolean(false);
        volatile double lossRate = 0.0;
    }

    static final class PartitionableTransport implements RaftTransport {
        private final RaftTransport delegate;
        private final LinkState link;
        private final java.util.Random rng = new java.util.Random();

        PartitionableTransport(RaftTransport delegate, LinkState link) {
            this.delegate = delegate;
            this.link = link;
        }

        private boolean shouldDrop() {
            return link.partitioned.get() || (link.lossRate > 0 && rng.nextDouble() < link.lossRate);
        }

        @Override public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest r) {
            return shouldDrop() ? failed() : delegate.requestVote(r);
        }
        @Override public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest r) {
            return shouldDrop() ? failed() : delegate.appendEntries(r);
        }
        @Override public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest r) {
            return shouldDrop() ? failed() : delegate.installSnapshot(r);
        }
        @Override public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest r) {
            return shouldDrop() ? failed() : delegate.preVote(r);
        }
        @Override public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest r) {
            return shouldDrop() ? failed() : delegate.timeoutNow(r);
        }
        @Override public void close() { delegate.close(); }

        private static <T> CompletableFuture<T> failed() {
            CompletableFuture<T> f = new CompletableFuture<>();
            f.completeExceptionally(new java.io.IOException("partitioned"));
            return f;
        }
    }
}
