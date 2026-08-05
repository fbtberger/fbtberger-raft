/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
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
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
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
 * Proves that {@code tls.mtls.enabled=true} actually REJECTS peers — the negative half of the
 * TLS story, which {@code TlsAndTimeoutsTest.grpcMtlsRoundTrip} does not cover.
 *
 * <p>Why this exists as its own test class: the pre-existing TLS tests all prove that a
 * <em>correct</em> connection succeeds. A green round-trip demonstrates that the lever is
 * wired, not that it holds anything back — an mTLS configuration that silently degraded to
 * "accept anyone" would pass every one of them. For a security control the rejection case
 * <em>is</em> the claim; the happy path is only the control group.
 *
 * <p>This exercises {@link GrpcTransportServer}, which is the transport actually used in
 * production ({@code RaftNodeConfiguration.transportFactory} builds a
 * {@link GrpcTransportFactory}). Note that {@code GrpcTransportServer} builds its SSL context
 * through {@code GrpcSslContexts} and therefore does NOT go through
 * {@link TlsConfig#buildServerSslContext()} — the two paths are tested separately
 * (see {@code NettyMtlsRejectionTest} for the other one).
 *
 * <p><b>What this test does NOT prove.</b> It proves membership of the cluster PKI, nothing
 * more. Both the legitimate and the rogue certificate below carry the same subject name and
 * differ only in their issuer, which is precisely the property mTLS checks. A certificate
 * signed by the cluster CA is accepted for <em>any</em> role, so a compromised learner key can
 * still open a connection and claim {@code leaderId=kwatro-1} in the payload — the
 * {@code leaderId}/{@code candidateId} fields are self-declared strings that never meet the
 * authenticated peer identity. Closing that gap is a separate change to
 * {@link RaftRpcHandler}, not something this test can assert today.
 */
class GrpcMtlsRejectionTest {

    private SelfSignedCertificate clusterCa;
    private SelfSignedCertificate foreignCa;
    private CountingHandler handler;
    private GrpcTransportServer server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        // Same CN on both, different issuer: the point of the test is that the NAME is worthless
        // and only the signing authority counts.
        clusterCa = new SelfSignedCertificate("localhost");
        foreignCa = new SelfSignedCertificate("localhost");
        handler = new CountingHandler();
        try (ServerSocket ss = new ServerSocket(0)) {
            port = ss.getLocalPort();
        }
        server = new GrpcTransportServer(port, handler, clusterTls());
        server.start();
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.close();
        }
        if (clusterCa != null) {
            clusterCa.delete();
        }
        if (foreignCa != null) {
            foreignCa.delete();
        }
    }

    private TlsConfig clusterTls() {
        return new TlsConfig(true, clusterCa.certificate(), clusterCa.privateKey(),
                clusterCa.certificate(), true);
    }

    /**
     * The control group. Without it, the two rejection tests below would stay green even if the
     * server were bound to the wrong port, never started, or refusing every connection for a
     * reason that has nothing to do with mTLS — all of which also produce "handler saw zero
     * calls". This test is what makes the other two mean something.
     */
    @Test
    @DisplayName("control: a peer with a cluster-CA certificate is accepted and reaches the handler")
    void acceptsPeerSignedByClusterCa() throws Exception {
        try (RaftTransport client = new GrpcTransportFactory(clusterTls()).connect("localhost:" + port)) {
            AppendEntriesResponse resp = client.appendEntries(appendEntries()).get(10, TimeUnit.SECONDS);
            assertTrue(resp.getSuccess(), "legitimate peer did not get a successful response");
        }
        assertEquals(1, handler.appendEntriesCalls.get(),
                "legitimate peer did not reach the Raft handler — the rejection tests below "
                        + "would be vacuous");
    }

    @Test
    @DisplayName("a peer holding a certificate from a foreign CA never reaches the Raft handler")
    void rejectsPeerSignedByForeignCa() throws Exception {
        assertRejected(GrpcSslContexts.forClient()
                .keyManager(foreignCa.certificate(), foreignCa.privateKey())
                .trustManager(clusterCa.certificate())
                .build());
    }

    @Test
    @DisplayName("a peer presenting no client certificate at all never reaches the Raft handler")
    void rejectsPeerWithoutClientCertificate() throws Exception {
        assertRejected(GrpcSslContexts.forClient()
                .trustManager(clusterCa.certificate())
                .build());
    }

    /**
     * Deliberately does not pin the exception type. Under TLS 1.3 a client-certificate failure is
     * reported to the client asynchronously, after its own handshake has already completed, so
     * whether this surfaces as an {@code ExecutionException} carrying a gRPC status or as a
     * timeout is a property of the negotiated protocol version, not of the security control. The
     * load-bearing assertion is the handler counter: the claim under test is "no Raft RPC from an
     * untrusted peer is ever executed", and that is what gets asserted.
     */
    private void assertRejected(io.grpc.netty.shaded.io.netty.handler.ssl.SslContext rogueSsl) {
        try (RaftTransport client = new GrpcTransport(
                NettyChannelBuilder.forTarget("localhost:" + port).sslContext(rogueSsl).build())) {
            try {
                AppendEntriesResponse resp = client.appendEntries(appendEntries()).get(10, TimeUnit.SECONDS);
                fail("untrusted peer got a response from the Raft transport: success="
                        + resp.getSuccess() + " term=" + resp.getTerm());
            } catch (Exception expected) {
                // Any failure mode is acceptable here; see the javadoc above.
            }
        }
        assertEquals(0, handler.appendEntriesCalls.get(),
                "an untrusted peer reached the Raft handler — mTLS did not reject it");
    }

    /** A leaderId a rogue peer would plausibly claim, to keep the scenario concrete. */
    private static AppendEntriesRequest appendEntries() {
        return AppendEntriesRequest.newBuilder()
                .setTerm(7)
                .setLeaderId("kwatro-1")
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
