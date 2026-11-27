/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.transport.GrpcTransport;
import com.fbtberger.raft.transport.GrpcTransportServer;
import com.fbtberger.raft.transport.RaftTransport;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The case §8 exists for: the leader commits a command, the client never hears of it, and
 * the retry lands on a different leader. Nothing on the new leader ever saw the first
 * submission as a client request -- it knows the command only from its log -- and it must
 * still answer the retry with the first result instead of applying it again.
 */
class RetryAcrossLeaderChangeTest {

    private static final Map<String, String> PEERS = Map.of(
            "d1", "dedup-d1", "d2", "dedup-d2", "d3", "dedup-d3");
    private static final byte[] INCREMENT = {1};

    private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
    private final Map<String, CountingStateMachine> machines = new LinkedHashMap<>();
    private final List<Server> servers = new ArrayList<>();
    private final List<RaftTransport> transports = new ArrayList<>();

    @BeforeEach
    void startCluster() throws Exception {
        for (Map.Entry<String, String> peer : PEERS.entrySet()) {
            CountingStateMachine machine = new CountingStateMachine();
            RaftConfig config = RaftConfig.of(peer.getKey(), 0, Path.of("unused"), PEERS);
            RaftNode node = new RaftNode(config, new InMemoryStorage(), machine, address -> {
                GrpcTransport transport = new GrpcTransport(
                        InProcessChannelBuilder.forName(address).directExecutor().build());
                synchronized (transports) {
                    transports.add(transport);
                }
                return transport;
            }, RaftMetrics.noop());
            machines.put(peer.getKey(), machine);
            nodes.put(peer.getKey(), node);
            servers.add(InProcessServerBuilder.forName(peer.getValue())
                    .addService(new GrpcTransportServer.RaftServiceAdapter(node)).build().start());
        }
        nodes.values().forEach(RaftNode::start);
    }

    @AfterEach
    void stopCluster() {
        nodes.values().forEach(RaftNode::shutdown);
        synchronized (transports) {
            transports.forEach(RaftTransport::close);
        }
        servers.forEach(Server::shutdownNow);
    }

    private String awaitLeaderOtherThan(String notThis) throws InterruptedException {
        return Await.until("a leader other than " + notThis, 15_000, () -> nodes.entrySet().stream()
                .filter(e -> !e.getKey().equals(notThis) && e.getValue().role() == ServerRole.LEADER)
                .map(Map.Entry::getKey).findFirst().orElse(null));
    }

    @Test
    void aRetryThatReachesTheNextLeaderIsNotAppliedAgain() throws Exception {
        String firstLeader = awaitLeaderOtherThan("nobody");
        byte[] first = nodes.get(firstLeader)
                .submitCommand(INCREMENT, "client-a", 1, 0).get(5, TimeUnit.SECONDS);

        String target = PEERS.keySet().stream().filter(id -> !id.equals(firstLeader)).findFirst().orElseThrow();
        nodes.get(firstLeader).transferLeadership(target).get(10, TimeUnit.SECONDS);
        String secondLeader = awaitLeaderOtherThan(firstLeader);

        byte[] retry = nodes.get(secondLeader)
                .submitCommand(INCREMENT, "client-a", 1, 0).get(5, TimeUnit.SECONDS);

        assertArrayEquals(first, retry, "the new leader answers from the session it rebuilt from the log");
        long retryIndex = nodes.get(secondLeader).commitIndex();
        for (Map.Entry<String, RaftNode> e : nodes.entrySet()) {
            Await.until(e.getKey() + " to apply the retry's entry", 5_000,
                    () -> e.getValue().appliedIndex() >= retryIndex);
            assertEquals(1, machines.get(e.getKey()).value(),
                    e.getKey() + " must have applied the command exactly once");
        }
    }
}
