/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.client;

import com.fbtberger.raft.client.proto.*;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How the client finds the leader — the part of {@link RaftClient} that decides anything.
 *
 * <p>Section 8 of the Raft paper says a client may talk to any server and be told where to go
 * instead. This class implements that: try a node, follow its {@code leader_hint}, remember the
 * hint for next time, and fall back to round-robin when there is no hint or the hint is stale.
 * All of it was untested — the class sat at 39 %, and what was dark was exactly the retry.
 *
 * <p>Real gRPC servers rather than mocks. The behaviour under test is "what does this client do
 * when a server answers X", and a mocked stub would have let the test assert that the client
 * calls the method it obviously calls. These stubs answer the way a follower and a leader answer,
 * and they record who was asked — which is the actual claim: the RIGHT node was contacted, in the
 * right order, and no more often than necessary.
 */
class RaftClientLeaderDiscoveryTest {

    private final List<Server> servers = new ArrayList<>();
    private final Map<String, String> cluster = new LinkedHashMap<>();
    private final Map<String, StubNode> nodes = new LinkedHashMap<>();

    @AfterEach
    void tearDown() {
        servers.forEach(Server::shutdownNow);
    }

    @Test
    @DisplayName("A follower's hint sends the client to the leader")
    void followsTheHint() throws Exception {
        start("n1", StubNode.followerPointingAt("n2"));
        start("n2", StubNode.leaderReturning("done"));

        try (RaftClient client = new RaftClient(cluster)) {
            assertArrayEquals("done".getBytes(StandardCharsets.UTF_8), client.submit(cmd()));
        }
        assertEquals(1, nodes.get("n1").asked.size(), "the follower should be asked once");
        assertEquals(1, nodes.get("n2").asked.size(), "the leader should be asked once");
    }

    /**
     * And it remembers. The point of the hint is that the NEXT command goes straight there;
     * a client that re-discovered the leader on every call would put a refusal in front of
     * every single command.
     */
    @Test
    @DisplayName("The hint is remembered, so the next command goes straight to the leader")
    void remembersTheLeader() throws Exception {
        start("n1", StubNode.followerPointingAt("n2"));
        start("n2", StubNode.leaderReturning("done"));

        try (RaftClient client = new RaftClient(cluster)) {
            client.submit(cmd());
            client.submit(cmd());
            client.submit(cmd());
        }
        assertEquals(1, nodes.get("n1").asked.size(),
                "the follower should have been asked only for the first command");
        assertEquals(3, nodes.get("n2").asked.size());
    }

    /**
     * A hint can be wrong — the node it names may have lost the election in the meantime, or may
     * not be in this client's address book at all. Then the client has to go back to trying
     * everyone rather than getting stuck on a name.
     */
    @Test
    @DisplayName("A hint naming an unknown node falls back to trying everyone")
    void staleHintFallsBack() throws Exception {
        start("n1", StubNode.followerPointingAt("n99"));   // not in the address book
        start("n2", StubNode.leaderReturning("done"));

        try (RaftClient client = new RaftClient(cluster)) {
            assertArrayEquals("done".getBytes(StandardCharsets.UTF_8), client.submit(cmd()));
        }
        assertTrue(nodes.get("n2").asked.size() >= 1, "the real leader has to be reached anyway");
    }

    @Test
    @DisplayName("When every node refuses, the client says so rather than hanging")
    void everyNodeRefuses() throws Exception {
        start("n1", StubNode.followerPointingAt(""));
        start("n2", StubNode.followerPointingAt(""));

        try (RaftClient client = new RaftClient(cluster)) {
            RaftClientException e = assertThrows(RaftClientException.class, () -> client.submit(cmd()));
            assertTrue(e.getMessage().contains("rejected"), e.getMessage());
        }
        assertEquals(1, nodes.get("n1").asked.size());
        assertEquals(1, nodes.get("n2").asked.size());
    }

    /**
     * A node that is not there is not a useful guess either. It has to be tried, and it must not
     * become the remembered leader.
     */
    @Test
    @DisplayName("An unreachable node is skipped and the reachable leader still answers")
    void unreachableNodeIsSkipped() throws Exception {
        cluster.put("gone", "localhost:" + freePort());   // nothing listening there
        start("n2", StubNode.leaderReturning("done"));

        try (RaftClient client = new RaftClient(cluster)) {
            assertArrayEquals("done".getBytes(StandardCharsets.UTF_8), client.submit(cmd()));
        }
    }

    @Test
    @DisplayName("A client with no addresses is refused at construction, not at first use")
    void emptyClusterIsRefused() {
        assertThrows(IllegalArgumentException.class, () -> new RaftClient(Map.of()));
    }

    @Test
    @DisplayName("A query follows the same discovery as a command")
    void queryAlsoFollowsTheHint() throws Exception {
        start("n1", StubNode.followerPointingAt("n2"));
        start("n2", StubNode.leaderReturning("value"));

        try (RaftClient client = new RaftClient(cluster)) {
            assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), client.query(cmd()));
        }
        assertEquals(1, nodes.get("n2").queried.size());
    }

    // ---- helpers ------------------------------------------------------------

    private static byte[] cmd() {
        return "SET k v".getBytes(StandardCharsets.UTF_8);
    }

    private void start(String id, StubNode stub) throws Exception {
        int port = freePort();
        Server s = ServerBuilder.forPort(port).addService(stub).build().start();
        servers.add(s);
        cluster.put(id, "localhost:" + port);
        nodes.put(id, stub);
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** A server that answers like a leader or like a follower, and remembers being asked. */
    private static final class StubNode extends RaftClientServiceGrpc.RaftClientServiceImplBase {
        final List<String> asked = new CopyOnWriteArrayList<>();
        final List<String> queried = new CopyOnWriteArrayList<>();
        private final boolean leader;
        private final String hint;
        private final String result;

        private StubNode(boolean leader, String hint, String result) {
            this.leader = leader; this.hint = hint; this.result = result;
        }

        static StubNode leaderReturning(String result)  { return new StubNode(true, "", result); }
        static StubNode followerPointingAt(String hint) { return new StubNode(false, hint, ""); }

        @Override
        public void submit(SubmitRequest request, StreamObserver<SubmitResponse> observer) {
            asked.add(request.getCommand().toStringUtf8());
            SubmitResponse.Builder b = SubmitResponse.newBuilder().setSuccess(leader);
            if (leader) b.setResult(ByteString.copyFromUtf8(result));
            else b.setLeaderHint(hint).setError("not leader");
            observer.onNext(b.build());
            observer.onCompleted();
        }

        @Override
        public void query(QueryRequest request, StreamObserver<QueryResponse> observer) {
            queried.add(request.getQuery().toStringUtf8());
            QueryResponse.Builder b = QueryResponse.newBuilder().setSuccess(leader);
            if (leader) b.setResult(ByteString.copyFromUtf8(result));
            else b.setLeaderHint(hint).setError("not leader");
            observer.onNext(b.build());
            observer.onCompleted();
        }
    }
}
