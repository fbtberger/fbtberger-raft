/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.client.RaftClient;
import com.fbtberger.raft.client.RaftClientException;
import com.fbtberger.raft.client.proto.AddServerRequest;
import com.fbtberger.raft.client.proto.RaftClientServiceGrpc;
import com.fbtberger.raft.client.proto.ReconfigurationResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Which rejection a failed reconfiguration reports.
 *
 * <p>The client walks every node until one accepts, so a failure has as many answers as
 * there are nodes -- and all but one of them are a follower saying "not leader", which
 * says nothing about the request. The leader's answer is the only one that does, and it
 * arrives first, so keeping the newest rejection guarantees the caller is shown the
 * least useful one.
 *
 * <p>This is not a cosmetic preference. On the Pi cluster a promotion refused by the
 * leader with "a previous configuration change has not committed yet; retry once it has"
 * was reported as "node1 rejected the request (not leader)". Twice, on different days,
 * that sent the investigation looking for an election -- with the term unchanged at 1
 * both times -- and it hid the fact that the library had asked, in those very words,
 * to be retried.
 *
 * <p>Real servers on real ports rather than in-process channels, because {@code
 * RaftClient} builds its own channels from the addresses it was given; an in-process
 * harness would test a client that cannot be constructed this way.
 */
class RaftClientReconfigureErrorTest {

    private final List<Server> servers = new java.util.ArrayList<>();

    @AfterEach
    void tearDown() {
        servers.forEach(Server::shutdownNow);
    }

    @Test
    void theLeadersReasonSurvivesTheFollowersThatSayNothing() throws Exception {
        String leaderReason = "a previous configuration change has not committed yet; retry once it has";

        // Insertion order is the walk order while no leader is known, so the leader is
        // asked first and the follower's "not leader" lands afterwards -- the exact
        // sequence that used to overwrite the answer worth reading.
        Map<String, String> cluster = new LinkedHashMap<>();
        cluster.put("leader", rejecting(leaderReason));
        cluster.put("follower", rejecting("not leader"));

        try (RaftClient client = new RaftClient(cluster)) {
            RaftClientException thrown =
                    assertThrows(RaftClientException.class, () -> client.addServer("node9", "pi9:9091"));

            assertTrue(thrown.getMessage().contains(leaderReason),
                    "the leader's reason was lost; got: " + thrown.getMessage());
            assertTrue(thrown.getMessage().startsWith("leader "),
                    "the reason must be attributed to the node that gave it; got: " + thrown.getMessage());
        }
    }

    @Test
    void withNothingButNotLeaderThatIsStillWhatIsReported() throws Exception {
        Map<String, String> cluster = new LinkedHashMap<>();
        cluster.put("one", rejecting("not leader"));
        cluster.put("two", rejecting("not leader"));

        // A cluster mid-election really does have nothing else to say, and inventing a
        // richer message for that case would be worse than the honest one.
        try (RaftClient client = new RaftClient(cluster)) {
            RaftClientException thrown =
                    assertThrows(RaftClientException.class, () -> client.addServer("node9", "pi9:9091"));
            assertTrue(thrown.getMessage().contains("not leader"), thrown.getMessage());
        }
    }

    @Test
    void anAcceptedRequestStillSucceedsAfterAFollowerRefuses() throws Exception {
        Map<String, String> cluster = new LinkedHashMap<>();
        cluster.put("follower", rejecting("not leader"));
        cluster.put("leader", accepting());

        try (RaftClient client = new RaftClient(cluster)) {
            client.addServer("node9", "pi9:9091");
            assertEquals("leader", client.knownLeaderId(),
                    "the node that accepted is the one to try first next time");
        }
    }

    /** A server that refuses every reconfiguration with the given reason. */
    private String rejecting(String reason) throws IOException {
        return serve(ReconfigurationResponse.newBuilder().setSuccess(false).setError(reason).build());
    }

    private String accepting() throws IOException {
        return serve(ReconfigurationResponse.newBuilder().setSuccess(true).build());
    }

    private String serve(ReconfigurationResponse response) throws IOException {
        Server server = ServerBuilder.forPort(0)
                .addService(new RaftClientServiceGrpc.RaftClientServiceImplBase() {
                    @Override
                    public void addServer(AddServerRequest request,
                                          StreamObserver<ReconfigurationResponse> observer) {
                        observer.onNext(response);
                        observer.onCompleted();
                    }
                })
                .build().start();
        servers.add(server);
        return "localhost:" + server.getPort();
    }
}
