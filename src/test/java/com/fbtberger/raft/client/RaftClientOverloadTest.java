/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.client;

import com.fbtberger.raft.client.proto.QueryRequest;
import com.fbtberger.raft.client.proto.QueryResponse;
import com.fbtberger.raft.client.proto.RaftClientServiceGrpc;
import com.fbtberger.raft.client.proto.SubmitRequest;
import com.fbtberger.raft.client.proto.SubmitResponse;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the client does when the leader says "overloaded": the same node again, later, with
 * the same command -- and not the next node, which can only be a follower.
 */
class RaftClientOverloadTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(2);

    private final Map<String, String> cluster = new LinkedHashMap<>();
    private final List<Server> servers = new ArrayList<>();
    private FakeNode leader;
    private FakeNode follower;
    private RaftClient client;

    @BeforeEach
    void start() throws Exception {
        leader = new FakeNode();
        follower = new FakeNode();
        for (Map.Entry<String, FakeNode> e : Map.of("n1", leader, "n2", follower).entrySet()) {
            Server server = ServerBuilder.forPort(0).addService(e.getValue()).build().start();
            servers.add(server);
            cluster.put(e.getKey(), "localhost:" + server.getPort());
        }
        // n1 first, whatever order the map above iterated in.
        Map<String, String> ordered = new LinkedHashMap<>();
        ordered.put("n1", cluster.get("n1"));
        ordered.put("n2", cluster.get("n2"));
        client = new RaftClient(ordered);
    }

    @AfterEach
    void stop() {
        if (client != null) client.close();
        servers.forEach(Server::shutdownNow);
    }

    @Test
    void anOverloadedLeaderIsAskedAgainAndTheFollowerIsLeftAlone() throws Exception {
        leader.overloadedAnswers.set(3);

        byte[] result = client.submit("SET a 1".getBytes(), TIMEOUT);

        assertArrayEquals("done".getBytes(), result);
        assertEquals(4, leader.submits.size(), "three refusals and the attempt that got through");
        assertEquals(0, follower.submits.size(), "a follower has nothing to offer an overloaded cluster");
    }

    @Test
    void everyAttemptCarriesTheSameSequenceSoTheCommandCannotBeAppliedTwice() throws Exception {
        leader.overloadedAnswers.set(2);

        client.submit("SET a 1".getBytes(), TIMEOUT);

        assertEquals(1, leader.submits.stream().map(SubmitRequest::getSequence).distinct().count());
        assertEquals(1, leader.submits.stream().map(SubmitRequest::getClientId).distinct().count());
    }

    @Test
    void aLeaderThatStaysOverloadedIsReportedAsSuchAndNotAsUnreachable() {
        leader.overloadedAnswers.set(Integer.MAX_VALUE);

        RaftClientException failure =
                assertThrows(RaftClientException.class, () -> client.submit("SET a 1".getBytes(), TIMEOUT));

        assertTrue(failure.overloaded(), "the caller has to be able to tell load from an outage");
        assertTrue(failure.getMessage().contains("overloaded"), failure.getMessage());
        assertEquals(RaftClient.MAX_OVERLOAD_RETRIES + 1, leader.submits.size());
        assertEquals(0, follower.submits.size());
    }

    @Test
    void readsBackOffTheSameWay() throws Exception {
        leader.overloadedAnswers.set(2);

        byte[] result = client.query("GET a".getBytes(), TIMEOUT);

        assertArrayEquals("done".getBytes(), result);
        assertEquals(3, leader.queries.get());
        assertEquals(0, follower.queries.get());
    }

    @Test
    void anOrdinaryFailureIsNotMarkedAsOverload() {
        leader.notLeader = true;
        follower.notLeader = true;

        RaftClientException failure =
                assertThrows(RaftClientException.class, () -> client.submit("SET a 1".getBytes(), TIMEOUT));

        assertTrue(!failure.overloaded());
    }

    /** Answers "overloaded" a set number of times, then succeeds. */
    private static final class FakeNode extends RaftClientServiceGrpc.RaftClientServiceImplBase {
        final AtomicInteger overloadedAnswers = new AtomicInteger();
        final List<SubmitRequest> submits = new CopyOnWriteArrayList<>();
        final AtomicInteger queries = new AtomicInteger();
        volatile boolean notLeader;

        @Override
        public void submit(SubmitRequest request, StreamObserver<SubmitResponse> observer) {
            submits.add(request);
            SubmitResponse.Builder response = SubmitResponse.newBuilder();
            if (notLeader) {
                response.setSuccess(false).setError("not leader");
            } else if (overloadedAnswers.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                response.setSuccess(false).setOverloaded(true).setRetryAfterMs(1).setError("overloaded");
            } else {
                response.setSuccess(true).setResult(ByteString.copyFromUtf8("done"));
            }
            observer.onNext(response.build());
            observer.onCompleted();
        }

        @Override
        public void query(QueryRequest request, StreamObserver<QueryResponse> observer) {
            queries.incrementAndGet();
            QueryResponse.Builder response = QueryResponse.newBuilder();
            if (overloadedAnswers.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
                response.setSuccess(false).setOverloaded(true).setRetryAfterMs(1).setError("overloaded");
            } else {
                response.setSuccess(true).setResult(ByteString.copyFromUtf8("done"));
            }
            observer.onNext(response.build());
            observer.onCompleted();
        }
    }
}
