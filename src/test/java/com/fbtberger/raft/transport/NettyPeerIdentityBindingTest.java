/*
 * Copyright 2026 fbtBerger Technology
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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The Netty counterpart of {@code GrpcPeerIdentityBindingTest}: under mTLS the sender id in a
 * Raft message must be the CN of the certificate the connection authenticated with.
 *
 * <p>The gRPC server has enforced this since the binding was introduced. The Netty server did
 * not: it demanded a client certificate and then never looked at whose it was, so on this
 * transport any cluster certificate could still send {@code leaderId=kwatro1}. The two servers
 * build their TLS and dispatch their RPCs in entirely separate code, so one being correct says
 * nothing about the other -- hence a test of its own rather than a parameter on the gRPC one.
 *
 * <p>As there, both peers hold certificates from the <em>same</em> CA. The transport trusts each
 * of them completely, so a refusal can only be the identity check. And as there, every refusal
 * is paired with a control on the same connection setup: a server that refuses everything would
 * otherwise pass.
 *
 * <p>A refusal on this transport is a closed connection (the protocol has no error frame), which
 * the client reports by failing the request. What the assertions pin is that the request fails
 * promptly and that the handler never saw it.
 */
class NettyPeerIdentityBindingTest {

    private TestPki pki;
    private TestPki.Node kwatro1;
    private TestPki.Node kwatro2;
    private CountingHandler handler;
    private NettyTransportServer server;
    private NioEventLoopGroup clientGroup;

    @BeforeEach
    void setUp() throws Exception {
        pki = TestPki.create("kwatro-cluster-ca");
        kwatro1 = pki.issue("kwatro1");
        kwatro2 = pki.issue("kwatro2");
        handler = new CountingHandler();
        clientGroup = new NioEventLoopGroup(1);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) server.close();
        if (clientGroup != null) clientGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        if (pki != null) pki.close();
    }

    /** A server as kwatro1, trusting the cluster CA -- so BOTH node certificates are valid to it. */
    private void startServer(TlsConfig tls) throws Exception {
        server = new NettyTransportServer(0, handler, tls);
        server.start();
    }

    /** A client presenting {@code identity}'s certificate. Trusted at the transport, every time. */
    private NettyTransport clientAs(TestPki.Node identity) throws Exception {
        return new NettyTransport("localhost", server.boundPort(), clientGroup,
                identity.tls(pki).buildClientSslContext());
    }

    @Test
    @DisplayName("control: kwatro2 claiming to be kwatro2 is accepted on all five RPCs")
    void acceptsAMatchingSenderId() throws Exception {
        startServer(kwatro1.tls(pki));
        NettyTransport client = clientAs(kwatro2);
        try {
            assertTrue(client.appendEntries(appendEntriesFrom("kwatro2")).get(10, TimeUnit.SECONDS)
                    .getSuccess(), "a peer that told the truth was rejected");
            client.requestVote(RequestVoteRequest.newBuilder().setTerm(2).setCandidateId("kwatro2").build())
                    .get(10, TimeUnit.SECONDS);
            client.preVote(PreVoteRequest.newBuilder().setTerm(2).setCandidateId("kwatro2").build())
                    .get(10, TimeUnit.SECONDS);
            client.installSnapshot(InstallSnapshotRequest.newBuilder()
                    .setTerm(1).setLeaderId("kwatro2").setDone(true).build()).get(10, TimeUnit.SECONDS);
            client.timeoutNow(TimeoutNowRequest.newBuilder().setTerm(2).setLeaderId("kwatro2").build())
                    .get(10, TimeUnit.SECONDS);
        } finally {
            client.close();
        }
        assertEquals(5, handler.total(), "every truthful call should have reached the handler");
    }

    /** THE POINT: an authentic connection, a forged sender. */
    @Test
    @DisplayName("kwatro2 claiming to be kwatro1 is refused, though its certificate is trusted")
    void rejectsAForgedLeaderId() throws Exception {
        assertRefused(c -> c.appendEntries(appendEntriesFrom("kwatro1")));
    }

    @Test
    @DisplayName("a forged candidateId is refused on RequestVote")
    void rejectsAForgedCandidateIdOnRequestVote() throws Exception {
        assertRefused(c -> c.requestVote(RequestVoteRequest.newBuilder()
                .setTerm(2).setCandidateId("kwatro1").build()));
    }

    @Test
    @DisplayName("a forged candidateId is refused on PreVote")
    void rejectsAForgedCandidateIdOnPreVote() throws Exception {
        assertRefused(c -> c.preVote(PreVoteRequest.newBuilder()
                .setTerm(2).setCandidateId("kwatro1").build()));
    }

    @Test
    @DisplayName("a forged leaderId is refused on InstallSnapshot")
    void rejectsAForgedLeaderIdOnInstallSnapshot() throws Exception {
        assertRefused(c -> c.installSnapshot(InstallSnapshotRequest.newBuilder()
                .setTerm(1).setLeaderId("kwatro1").setDone(true).build()));
    }

    @Test
    @DisplayName("a forged leaderId is refused on TimeoutNow")
    void rejectsAForgedLeaderIdOnTimeoutNow() throws Exception {
        assertRefused(c -> c.timeoutNow(TimeoutNowRequest.newBuilder()
                .setTerm(2).setLeaderId("kwatro1").build()));
    }

    /** Leaving the sender out must not be a way around the check. */
    @Test
    @DisplayName("a TimeoutNow without a leaderId is refused under mTLS")
    void rejectsATimeoutNowWithoutASender() throws Exception {
        assertRefused(c -> c.timeoutNow(TimeoutNowRequest.newBuilder().setTerm(2).build()));
    }

    /** A refused sender loses the connection, so nothing it sends afterwards is served either. */
    @Test
    @DisplayName("after a forged request the connection is gone")
    void aForgedRequestCostsTheConnection() throws Exception {
        startServer(kwatro1.tls(pki));
        NettyTransport client = clientAs(kwatro2);
        try {
            assertFails(client.appendEntries(appendEntriesFrom("kwatro1")));
            assertFails(client.appendEntries(appendEntriesFrom("kwatro2")));
        } finally {
            client.close();
        }
        assertEquals(0, handler.total());
    }

    @Test
    @DisplayName("with one-way TLS the sender id is not checked")
    void oneWayTlsIsUnaffected() throws Exception {
        startServer(new TlsConfig(true, kwatro1.certFile(), kwatro1.keyFile(), pki.caFile(), false));
        NettyTransport client = clientAs(kwatro2);
        try {
            assertTrue(client.appendEntries(appendEntriesFrom("anything-at-all"))
                    .get(10, TimeUnit.SECONDS).getSuccess());
        } finally {
            client.close();
        }
        assertEquals(1, handler.total());
    }

    @Test
    @DisplayName("with TLS off the sender id is not checked")
    void plaintextClusterIsUnaffected() throws Exception {
        startServer(null);
        NettyTransport client = new NettyTransport("localhost", server.boundPort(), clientGroup, null);
        try {
            assertTrue(client.appendEntries(appendEntriesFrom("anything-at-all"))
                    .get(10, TimeUnit.SECONDS).getSuccess());
            client.timeoutNow(TimeoutNowRequest.newBuilder().setTerm(2).build()).get(10, TimeUnit.SECONDS);
        } finally {
            client.close();
        }
        assertEquals(2, handler.total());
    }

    private void assertRefused(Function<NettyTransport, CompletableFuture<?>> call) throws Exception {
        startServer(kwatro1.tls(pki));
        NettyTransport client = clientAs(kwatro2);
        try {
            assertFails(call.apply(client));
        } finally {
            client.close();
        }
        assertEquals(0, handler.total(),
                "the forged request reached the handler -- the binding did not hold");
    }

    private static void assertFails(CompletableFuture<?> future) throws Exception {
        try {
            future.get(10, TimeUnit.SECONDS);
            fail("the call was accepted; the sender-id binding did not fire");
        } catch (TimeoutException e) {
            fail("the refused call was left pending instead of failing");
        } catch (ExecutionException expected) {
            // refused: the server closed the connection
        }
    }

    private static AppendEntriesRequest appendEntriesFrom(String leaderId) {
        return AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId(leaderId)
                .setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0)
                .build();
    }

    /** Counts what actually reached the handler -- a refusal must leave it at zero. */
    private static final class CountingHandler implements RaftRpcHandler {
        private final AtomicInteger calls = new AtomicInteger();

        int total() {
            return calls.get();
        }

        @Override
        public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
            calls.incrementAndGet();
            return AppendEntriesResponse.newBuilder().setTerm(request.getTerm()).setSuccess(true).build();
        }

        @Override
        public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
            calls.incrementAndGet();
            return RequestVoteResponse.newBuilder().setTerm(request.getTerm()).setVoteGranted(true).build();
        }

        @Override
        public PreVoteResponse handlePreVote(PreVoteRequest request) {
            calls.incrementAndGet();
            return PreVoteResponse.newBuilder().setTerm(request.getTerm()).setVoteGranted(true).build();
        }

        @Override
        public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
            calls.incrementAndGet();
            return InstallSnapshotResponse.newBuilder().setTerm(request.getTerm()).build();
        }

        @Override
        public TimeoutNowResponse handleTimeoutNow(TimeoutNowRequest request) {
            calls.incrementAndGet();
            return TimeoutNowResponse.newBuilder().build();
        }
    }
}
