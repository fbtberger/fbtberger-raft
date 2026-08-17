/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.client.proto.RaftClientServiceGrpc;
import com.fbtberger.raft.client.proto.SubmitRequest;
import com.fbtberger.raft.client.proto.SubmitResponse;
import com.fbtberger.raft.client.proto.AddServerRequest;
import com.fbtberger.raft.client.proto.RemoveServerRequest;
import com.fbtberger.raft.client.proto.ReconfigurationResponse;
import com.fbtberger.raft.transport.GrpcTransport;
import com.fbtberger.raft.transport.GrpcTransportServer;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RaftClientGrpcServiceTest {

    private static final String SERVER_NAME = "client-test-server";

    private RaftNode node;
    private Server grpcServer;
    private ManagedChannel channel;
    private RaftClientServiceGrpc.RaftClientServiceBlockingStub clientStub;

    @BeforeEach
    void setUp() throws Exception {
        InMemoryStorage store = new InMemoryStorage();
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-client-test");
        props.setProperty("peer.n1", SERVER_NAME);
        props.setProperty("snapshot.threshold", "100");
        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-client-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        RaftConfig config = RaftConfig.load(tmp);

        node = new RaftNode(config, store, new KeyValueStateMachine(),
                addr -> new GrpcTransport(InProcessChannelBuilder.forName(addr).directExecutor().build()),
                RaftMetrics.noop());

        RaftClientGrpcService clientService = new RaftClientGrpcService(node);
        grpcServer = InProcessServerBuilder.forName(SERVER_NAME)
                .directExecutor()
                .addService(new GrpcTransportServer.RaftServiceAdapter(node))
                .addService(clientService)
                .build().start();

        node.start();
        long deadline = System.currentTimeMillis() + 1_000;
        while (node.role() != ServerRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        channel = InProcessChannelBuilder.forName(SERVER_NAME).directExecutor().build();
        clientStub = RaftClientServiceGrpc.newBlockingStub(channel);
    }

    @AfterEach
    void tearDown() {
        node.shutdown();
        channel.shutdownNow();
        grpcServer.shutdown();
    }

    @Test
    void submitCommandSucceeds() {
        SubmitResponse resp = clientStub.submit(SubmitRequest.newBuilder()
                .setCommand(ByteString.copyFromUtf8("SET x 42"))
                .build());
        assertTrue(resp.getSuccess());
        assertEquals("OK", resp.getResult().toStringUtf8());
    }

    @Test
    void submitToFollowerReturnsNotLeader() throws Exception {
        RaftConfig followerConfig = loadConfig("follower", "19999",
                java.util.Map.of("follower", "localhost:19999", "other1", "localhost:19998", "other2", "localhost:19997"));
        RaftNode follower = new RaftNode(followerConfig, new InMemoryStorage(),
                new KeyValueStateMachine(), addr -> null, RaftMetrics.noop());
        RaftClientGrpcService clientService = new RaftClientGrpcService(follower);

        String followerServer = "follower-test-server";
        Server srv = InProcessServerBuilder.forName(followerServer)
                .directExecutor().addService(clientService).build().start();
        ManagedChannel ch = InProcessChannelBuilder.forName(followerServer).directExecutor().build();
        try {
            RaftClientServiceGrpc.RaftClientServiceBlockingStub stub =
                    RaftClientServiceGrpc.newBlockingStub(ch);
            SubmitResponse resp = stub.submit(SubmitRequest.newBuilder()
                    .setCommand(ByteString.copyFromUtf8("SET x 1")).build());
            assertFalse(resp.getSuccess());
            assertEquals("not leader", resp.getError());
        } finally {
            follower.shutdown();
            ch.shutdownNow();
            srv.shutdown();
        }
    }

    @Test
    void addServerThenRemoveServer() {
        ReconfigurationResponse addResp = clientStub.addServer(AddServerRequest.newBuilder()
                .setId("n2").setAddress("localhost:9092").build());
        assertTrue(addResp.getSuccess());
        assertTrue(node.currentConfiguration().containsKey("n2"));

        ReconfigurationResponse rmResp = clientStub.removeServer(RemoveServerRequest.newBuilder()
                .setId("n2").build());
        assertTrue(rmResp.getSuccess());
        assertFalse(node.currentConfiguration().containsKey("n2"));
    }

    @Test
    void addDuplicateServerFails() {
        ReconfigurationResponse resp = clientStub.addServer(AddServerRequest.newBuilder()
                .setId("n1").setAddress("localhost:9091").build());
        assertFalse(resp.getSuccess());
        assertNotNull(resp.getError());
    }

    @Test
    void removeNonMemberFails() {
        ReconfigurationResponse resp = clientStub.removeServer(RemoveServerRequest.newBuilder()
                .setId("nobody").build());
        assertFalse(resp.getSuccess());
    }

    private static RaftConfig loadConfig(String id, String port, java.util.Map<String, String> peers) throws Exception {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", id);
        props.setProperty("node.port", port);
        props.setProperty("data.dir", "/tmp/raft-test-" + id);
        props.setProperty("snapshot.threshold", "100");
        for (var e : peers.entrySet()) {
            props.setProperty("peer." + e.getKey(), e.getValue());
        }
        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-test-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }
}
