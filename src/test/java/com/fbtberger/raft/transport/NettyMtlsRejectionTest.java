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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The same rejection claim as {@code GrpcMtlsRejectionTest}, but for the Netty transport.
 *
 * <p>These are genuinely two different code paths and one does not stand in for the other:
 * {@link NettyTransportServer} builds its SSL context via {@link TlsConfig#buildServerSslContext()}
 * (Netty's own {@code SslContextBuilder}, pinned to {@link SslProvider#JDK}), whereas
 * {@link GrpcTransportServer} builds its own through {@code GrpcSslContexts} and never touches
 * that method. {@code mtlsEnabled()} is honoured in both places, separately — so it can be
 * correct in one and wrong in the other.
 *
 * <p>Like the gRPC variant this is a test about <em>trust</em>, not identity: the legitimate and
 * the rogue certificate carry the same node id and differ only in who signed them. Since the
 * Netty server binds the sender id to the certificate CN, a certificate with an unrelated CN
 * would be refused for that reason instead, and the test would stop saying anything about the CA.
 * Identity is {@code NettyPeerIdentityBindingTest}'s subject.
 */
class NettyMtlsRejectionTest {

    private static final String LEADER_ID = "kwatro-1";

    private TestPki clusterPki;
    private TestPki foreignPki;
    private TestPki.Node legitimate;
    private TestPki.Node rogue;
    private CountingHandler handler;
    private NettyTransportServer server;
    private NioEventLoopGroup clientGroup;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        clusterPki = TestPki.create("kwatro-cluster-ca");
        foreignPki = TestPki.create("some-other-ca");
        legitimate = clusterPki.issue(LEADER_ID);
        rogue = foreignPki.issue(LEADER_ID);
        handler = new CountingHandler();
        clientGroup = new NioEventLoopGroup(1);
        try (ServerSocket ss = new ServerSocket(0)) {
            port = ss.getLocalPort();
        }
        server = new NettyTransportServer(port, handler, clusterTls());
        server.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.close();
        }
        if (clientGroup != null) {
            clientGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
        if (clusterPki != null) {
            clusterPki.close();
        }
        if (foreignPki != null) {
            foreignPki.close();
        }
    }

    private TlsConfig clusterTls() {
        return legitimate.tls(clusterPki);
    }

    /** Control group — see the equivalent test in {@code GrpcMtlsRejectionTest} for why. */
    @Test
    @DisplayName("control: a peer with a cluster-CA certificate is accepted and reaches the handler")
    void acceptsPeerSignedByClusterCa() throws Exception {
        SslContext clientSsl = clusterTls().buildClientSslContext();
        NettyTransport client = new NettyTransport("localhost", port, clientGroup, clientSsl);
        try {
            AppendEntriesResponse resp = client.appendEntries(appendEntries()).get(10, TimeUnit.SECONDS);
            assertTrue(resp.getSuccess(), "legitimate peer did not get a successful response");
        } finally {
            client.close();
        }
        assertEquals(1, handler.appendEntriesCalls.get(),
                "legitimate peer did not reach the Raft handler — the rejection test below "
                        + "would be vacuous");
    }

    @Test
    @DisplayName("a peer holding a certificate from a foreign CA never reaches the Raft handler")
    void rejectsPeerSignedByForeignCa() throws Exception {
        assertRejected(SslContextBuilder.forClient()
                .sslProvider(SslProvider.JDK)
                .keyManager(rogue.certFile(), rogue.keyFile())
                .trustManager(clusterPki.caFile())
                .build());
    }

    @Test
    @DisplayName("a peer presenting no client certificate at all never reaches the Raft handler")
    void rejectsPeerWithoutClientCertificate() throws Exception {
        assertRejected(SslContextBuilder.forClient()
                .sslProvider(SslProvider.JDK)
                .trustManager(clusterPki.caFile())
                .build());
    }

    /**
     * As in the gRPC variant, the exception type is not pinned: under TLS 1.3 the rejection can
     * surface as a failed handshake, a failed write or a closed connection. What is pinned is
     * that it surfaces at all -- {@link NettyTransport} fails everything pending when the channel
     * closes, so a timeout here would be a defect, not a plausible outcome.
     */
    private void assertRejected(SslContext rogueSsl) {
        NettyTransport client = new NettyTransport("localhost", port, clientGroup, rogueSsl);
        try {
            AppendEntriesResponse resp = client.appendEntries(appendEntries()).get(10, TimeUnit.SECONDS);
            fail("untrusted peer got a response from the Raft transport: success="
                    + resp.getSuccess() + " term=" + resp.getTerm());
        } catch (java.util.concurrent.TimeoutException hung) {
            fail("the request to a rejecting peer was still pending after 10s");
        } catch (Exception expected) {
            // Any other failure mode is acceptable here; see the javadoc above.
        } finally {
            client.close();
        }
        assertEquals(0, handler.appendEntriesCalls.get(),
                "an untrusted peer reached the Raft handler — mTLS did not reject it");
    }

    private static AppendEntriesRequest appendEntries() {
        return AppendEntriesRequest.newBuilder()
                .setTerm(7)
                .setLeaderId(LEADER_ID)
                .setPrevLogIndex(0)
                .setPrevLogTerm(0)
                .build();
    }

    private static final class CountingHandler implements RaftRpcHandler {

        final AtomicInteger appendEntriesCalls = new AtomicInteger();

        @Override
        public RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
            return RequestVoteResponse.newBuilder().setTerm(req.getTerm()).setVoteGranted(true).build();
        }

        @Override
        public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
            appendEntriesCalls.incrementAndGet();
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
            return TimeoutNowResponse.newBuilder().build();
        }
    }
}
