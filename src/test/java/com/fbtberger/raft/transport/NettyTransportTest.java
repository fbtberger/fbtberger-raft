/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.PreVoteRequest;
import com.fbtberger.raft.proto.PreVoteResponse;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class NettyTransportTest {

    private static final int PORT = 19876;

    private NettyTransportServer server;
    private NettyTransport client;
    private final NioEventLoopGroup clientGroup = new NioEventLoopGroup(1);

    private final StubHandler handler = new StubHandler();

    @BeforeEach
    void setUp() throws Exception {
        server = new NettyTransportServer(PORT, handler);
        server.start();
        client = new NettyTransport("localhost", PORT, clientGroup, null);
    }

    @AfterEach
    void tearDown() {
        client.close();
        server.close();
        clientGroup.shutdownGracefully();
    }

    @Test
    void requestVoteRoundTrip() throws Exception {
        RequestVoteRequest req = RequestVoteRequest.newBuilder()
                .setTerm(5).setCandidateId("n1").setLastLogIndex(10).setLastLogTerm(4).build();
        RequestVoteResponse resp = client.requestVote(req).get(2, TimeUnit.SECONDS);
        assertEquals(5, resp.getTerm());
        assertTrue(resp.getVoteGranted());
    }

    @Test
    void appendEntriesRoundTrip() throws Exception {
        AppendEntriesRequest req = AppendEntriesRequest.newBuilder()
                .setTerm(3).setLeaderId("leader").setPrevLogIndex(0).setPrevLogTerm(0).build();
        AppendEntriesResponse resp = client.appendEntries(req).get(2, TimeUnit.SECONDS);
        assertEquals(3, resp.getTerm());
        assertTrue(resp.getSuccess());
    }

    @Test
    void installSnapshotRoundTrip() throws Exception {
        InstallSnapshotRequest req = InstallSnapshotRequest.newBuilder()
                .setTerm(2).setLeaderId("leader").setLastIncludedIndex(5).setLastIncludedTerm(2)
                .setOffset(0).setDone(true).build();
        InstallSnapshotResponse resp = client.installSnapshot(req).get(2, TimeUnit.SECONDS);
        assertEquals(2, resp.getTerm());
    }

    @Test
    void preVoteRoundTrip() throws Exception {
        PreVoteRequest req = PreVoteRequest.newBuilder()
                .setTerm(4).setCandidateId("n2").setLastLogIndex(3).setLastLogTerm(2).build();
        PreVoteResponse resp = client.preVote(req).get(2, TimeUnit.SECONDS);
        assertEquals(4, resp.getTerm());
        assertTrue(resp.getVoteGranted());
    }

    @Test
    void timeoutNowRoundTrip() throws Exception {
        TimeoutNowRequest req = TimeoutNowRequest.newBuilder().setTerm(7).build();
        TimeoutNowResponse resp = client.timeoutNow(req).get(2, TimeUnit.SECONDS);
        assertNotNull(resp);
    }

    @Test
    void multipleSequentialRequests() throws Exception {
        for (int i = 1; i <= 5; i++) {
            AppendEntriesRequest req = AppendEntriesRequest.newBuilder()
                    .setTerm(i).setLeaderId("leader").build();
            AppendEntriesResponse resp = client.appendEntries(req).get(2, TimeUnit.SECONDS);
            assertEquals(i, resp.getTerm());
        }
    }

    @Test
    void concurrentRequests() throws Exception {
        var futures = new java.util.ArrayList<java.util.concurrent.CompletableFuture<?>>();
        for (int i = 0; i < 10; i++) {
            final int term = i + 1;
            futures.add(client.appendEntries(AppendEntriesRequest.newBuilder()
                    .setTerm(term).setLeaderId("leader").build()));
        }
        for (var f : futures) {
            assertNotNull(f.get(2, TimeUnit.SECONDS));
        }
    }

    private static final class StubHandler implements RaftRpcHandler {
        @Override
        public RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
            return RequestVoteResponse.newBuilder().setTerm(req.getTerm()).setVoteGranted(true).build();
        }

        @Override
        public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
            return AppendEntriesResponse.newBuilder().setTerm(req.getTerm()).setSuccess(true).build();
        }

        @Override
        public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest req) {
            return InstallSnapshotResponse.newBuilder().setTerm(req.getTerm()).build();
        }

        @Override
        public PreVoteResponse handlePreVote(PreVoteRequest req) {
            return PreVoteResponse.newBuilder().setTerm(req.getTerm()).setVoteGranted(true).build();
        }

        @Override
        public TimeoutNowResponse handleTimeoutNow(TimeoutNowRequest req) {
            return TimeoutNowResponse.getDefaultInstance();
        }
    }
}
