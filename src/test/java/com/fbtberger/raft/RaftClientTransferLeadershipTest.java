/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.client.RaftClient;
import com.fbtberger.raft.client.RaftClientException;
import com.fbtberger.raft.client.proto.RaftClientServiceGrpc;
import com.fbtberger.raft.client.proto.ReconfigurationResponse;
import com.fbtberger.raft.client.proto.TransferLeadershipRequest;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What {@link RaftClient#transferLeadership} actually puts on the wire, and where it puts it.
 *
 * <p>The leader-following walk itself is shared with the reconfiguration calls and tested
 * with them; repeating that here would test the same loop twice. What is not shared, and
 * what no other test can see, is this call's own wiring: which RPC it invokes and which
 * field it fills. Both are the kind of mistake that compiles, passes every mock-based
 * check, and then hands the lead to nobody -- {@code target_id} left empty is a perfectly
 * valid protobuf message.
 *
 * <p>Real servers on real ports, because {@code RaftClient} builds its own channels from
 * the addresses it is given and cannot be pointed at an in-process harness.
 */
class RaftClientTransferLeadershipTest {

    private final List<Server> servers = new ArrayList<>();

    @AfterEach
    void tearDown() {
        servers.forEach(Server::shutdownNow);
    }

    @Test
    void theNamedTargetIsWhatReachesTheLeader() throws Exception {
        AtomicReference<String> seen = new AtomicReference<>();

        Map<String, String> cluster = new LinkedHashMap<>();
        cluster.put("leader", serve(ReconfigurationResponse.newBuilder().setSuccess(true).build(), seen));

        try (RaftClient client = new RaftClient(cluster)) {
            client.transferLeadership("node1");
            assertEquals("node1", seen.get(),
                    "the target the operator named must be the target the leader is told about");
        }
    }

    @Test
    void aRefusalIsReportedWithTheLeadersReason() throws Exception {
        String reason = "node9 is not a current cluster member";

        Map<String, String> cluster = new LinkedHashMap<>();
        cluster.put("leader", serve(
                ReconfigurationResponse.newBuilder().setSuccess(false).setError(reason).build(),
                new AtomicReference<>()));

        try (RaftClient client = new RaftClient(cluster)) {
            RaftClientException thrown = assertThrows(RaftClientException.class,
                    () -> client.transferLeadership("node9"));
            assertTrue(thrown.getMessage().contains(reason), thrown.getMessage());
        }
    }

    @Test
    void afterAFollowerRefusesTheWalkStillFindsTheLeader() throws Exception {
        AtomicReference<String> seen = new AtomicReference<>();

        Map<String, String> cluster = new LinkedHashMap<>();
        cluster.put("follower", serve(
                ReconfigurationResponse.newBuilder().setSuccess(false).setError("not leader").build(),
                new AtomicReference<>()));
        cluster.put("leader", serve(ReconfigurationResponse.newBuilder().setSuccess(true).build(), seen));

        try (RaftClient client = new RaftClient(cluster)) {
            client.transferLeadership("node1");
            assertEquals("node1", seen.get(), "the leader must have been asked, not just the follower");
            assertEquals("leader", client.knownLeaderId(),
                    "the node that accepted is the one to try first next time");
        }
    }

    /** A server answering every transfer alike, recording the target it was handed. */
    private String serve(ReconfigurationResponse response, AtomicReference<String> seen) throws IOException {
        Server server = ServerBuilder.forPort(0)
                .addService(new RaftClientServiceGrpc.RaftClientServiceImplBase() {
                    @Override
                    public void transferLeadership(TransferLeadershipRequest request,
                                                   StreamObserver<ReconfigurationResponse> observer) {
                        seen.set(request.getTargetId());
                        observer.onNext(response);
                        observer.onCompleted();
                    }
                })
                .build().start();
        servers.add(server);
        return "localhost:" + server.getPort();
    }
}
