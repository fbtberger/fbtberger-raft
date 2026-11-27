/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.client;

import com.fbtberger.raft.InMemoryStorage;
import com.fbtberger.raft.KeyValueStateMachine;
import com.fbtberger.raft.RaftClientGrpcService;
import com.fbtberger.raft.RaftConfig;
import com.fbtberger.raft.RaftMetrics;
import com.fbtberger.raft.RaftNode;
import com.fbtberger.raft.ServerRole;
import com.fbtberger.raft.transport.GrpcTransportFactory;
import com.fbtberger.raft.transport.GrpcTransportServer;
import com.fbtberger.raft.transport.RaftTransport;
import com.fbtberger.raft.transport.TlsConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ReadMode#FOLLOWER} from the client's side, against three real nodes on real ports:
 * the reads are answered, they are answered by nodes that are not the leader, they see the
 * write that preceded them, and they do not confuse the client about who the leader is.
 */
class RaftClientFollowerReadTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(5);

    private final Map<String, String> cluster = new LinkedHashMap<>();
    private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
    private final Map<String, RaftMetrics> metrics = new LinkedHashMap<>();
    private final List<GrpcTransportServer> servers = new ArrayList<>();
    private final List<RaftTransport> transports = new ArrayList<>();

    @BeforeEach
    void startCluster() throws Exception {
        Map<String, Integer> ports = new LinkedHashMap<>();
        for (String id : List.of("r1", "r2", "r3")) {
            try (ServerSocket socket = new ServerSocket(0)) {
                ports.put(id, socket.getLocalPort());
            }
            cluster.put(id, "localhost:" + ports.get(id));
        }
        GrpcTransportFactory factory = new GrpcTransportFactory(TlsConfig.disabled());
        for (String id : cluster.keySet()) {
            RaftConfig config = RaftConfig.of(id, ports.get(id), Path.of("unused"), cluster);
            RaftMetrics nodeMetrics = new RaftMetrics(new SimpleMeterRegistry(), id);
            RaftNode node = new RaftNode(config, new InMemoryStorage(), new KeyValueStateMachine(), address -> {
                RaftTransport transport = factory.connect(address);
                synchronized (transports) {
                    transports.add(transport);
                }
                return transport;
            }, nodeMetrics);
            nodes.put(id, node);
            metrics.put(id, nodeMetrics);
            GrpcTransportServer server = new GrpcTransportServer(ports.get(id), node, TlsConfig.disabled(),
                    List.of(new RaftClientGrpcService(node).bindService()));
            server.start();
            servers.add(server);
        }
        nodes.values().forEach(RaftNode::start);
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline && !(leaderId() != null
                && nodes.values().stream().allMatch(n -> leaderId().equals(n.currentLeaderId())))) {
            Thread.sleep(20);
        }
        assertTrue(leaderId() != null, "no leader was elected");
    }

    @AfterEach
    void stopCluster() {
        nodes.values().forEach(RaftNode::shutdown);
        synchronized (transports) {
            transports.forEach(RaftTransport::close);
        }
        servers.forEach(GrpcTransportServer::close);
    }

    private String leaderId() {
        return nodes.entrySet().stream().filter(e -> e.getValue().role() == ServerRole.LEADER)
                .map(Map.Entry::getKey).findFirst().orElse(null);
    }

    private double followerReads(String nodeId) {
        return metrics.get(nodeId).registry().get("raft.read.follower").counter().count();
    }

    @Test
    void followerReadsSeeEveryPrecedingWriteAndAreServedOffTheLeader() throws Exception {
        String leader = leaderId();
        try (RaftClient client = new RaftClient(cluster)) {
            for (int i = 1; i <= 12; i++) {
                client.submit(("SET k " + i).getBytes(StandardCharsets.UTF_8), TIMEOUT);

                byte[] answer = client.query("GET k".getBytes(StandardCharsets.UTF_8), TIMEOUT, ReadMode.FOLLOWER);

                assertEquals(String.valueOf(i), new String(answer, StandardCharsets.UTF_8));
                assertEquals(leader, client.knownLeaderId(),
                        "a follower that answered a read must not be remembered as the leader");
            }
        }
        assertEquals(0, followerReads(leader), "the leader was only the fallback and was never needed");
        double total = 0;
        for (String id : cluster.keySet()) {
            if (!id.equals(leader)) {
                assertTrue(followerReads(id) >= 1, id + " never served a read: the client does not rotate");
                total += followerReads(id);
            }
        }
        assertEquals(12, total);
        assertEquals(12, metrics.get(leader).registry().get("raft.read.index.served").counter().count(),
                "each follower read costs the leader one ReadIndex and no read");
    }

    @Test
    void aClientThatDoesNotKnowTheLeaderStillGetsAFollowerRead() throws Exception {
        nodes.get(leaderId()).submitCommand("SET k v".getBytes(StandardCharsets.UTF_8)).get();

        try (RaftClient fresh = new RaftClient(cluster)) {
            byte[] answer = fresh.query("GET k".getBytes(StandardCharsets.UTF_8), TIMEOUT, ReadMode.FOLLOWER);

            assertEquals("v", new String(answer, StandardCharsets.UTF_8));
        }
    }
}
