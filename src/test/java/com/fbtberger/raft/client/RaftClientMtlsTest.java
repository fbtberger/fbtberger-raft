/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.client;

import com.fbtberger.raft.client.proto.RaftClientServiceGrpc;
import com.fbtberger.raft.client.proto.ReconfigurationResponse;
import com.fbtberger.raft.client.proto.SubmitRequest;
import com.fbtberger.raft.client.proto.SubmitResponse;
import com.fbtberger.raft.transport.TestPki;
import com.fbtberger.raft.transport.TlsConfig;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * That the client channel can speak mTLS, and that the server's refusals reach the caller.
 *
 * <p>The peer transport has had TLS since it was written and runs with mTLS on the kwatro
 * production cluster -- all five members. This channel had no option at all:
 * {@code usePlaintext()} was hard-coded, so the writes the web tier forwards with
 * {@code SubmitRaftCommand}, and every membership change an operator makes with
 * {@code membership.sh}, crossed the same private network in the clear while the heartbeats
 * beside them were encrypted and mutually authenticated. Nobody decided that; a constructor
 * simply could not express the other case.
 *
 * <p>Both directions are asserted. A test that only shows the good certificate working would
 * pass just as well on a server that accepts anything, which is the failure worth catching --
 * and the one this repo's {@code GrpcMtlsRejectionTest} already guards for the peer side.
 */
class RaftClientMtlsTest {

    private TestPki cluster;
    private TestPki foreign;
    private Server server;
    private Map<String, String> addresses;

    @BeforeEach
    void setUp() throws Exception {
        cluster = TestPki.create("kwatro-cluster-ca");
        foreign = TestPki.create("some-other-ca");

        TestPki.Node serverCert = cluster.issue("node1");
        server = NettyServerBuilder.forPort(0)
                .sslContext(GrpcSslContexts.forServer(serverCert.certFile(), serverCert.keyFile())
                        .trustManager(cluster.caFile())
                        .clientAuth(ClientAuth.REQUIRE)
                        .build())
                .addService(new EchoingClientService())
                .build().start();

        addresses = new LinkedHashMap<>();
        addresses.put("node1", "localhost:" + server.getPort());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) {
            server.shutdownNow();
        }
        if (cluster != null) {
            cluster.close();
        }
        if (foreign != null) {
            foreign.close();
        }
    }

    @Test
    void aClientHoldingAClusterCertificateGetsThrough() throws Exception {
        TlsConfig tls = cluster.issue("operator").tls(cluster);

        try (RaftClient client = new RaftClient(addresses, tls)) {
            byte[] result = client.submit("SET a 1".getBytes(StandardCharsets.UTF_8));
            assertArrayEquals("OK".getBytes(StandardCharsets.UTF_8), result,
                    "a peer holding a certificate from the cluster CA was refused");
        }
    }

    @Test
    void aClientHoldingAForeignCertificateIsRefused() throws Exception {
        // Signed by a CA the server does not trust. The handshake fails, so the call never
        // reaches the service -- which is why this is a transport property and not something
        // the service could enforce.
        TestPki.Node stranger = foreign.issue("operator");
        TlsConfig tls = new TlsConfig(true, stranger.certFile(), stranger.keyFile(),
                foreign.caFile(), true);

        try (RaftClient client = new RaftClient(addresses, tls)) {
            assertThrows(RaftClientException.class,
                    () -> client.submit("SET a 1".getBytes(StandardCharsets.UTF_8)),
                    "a certificate from a foreign CA was accepted");
        }
    }

    @Test
    void aPlaintextClientCannotTalkToATlsCluster() throws Exception {
        // The old constructor, unchanged, against a cluster that now requires certificates.
        // It must fail rather than half-work: an operator tool that silently talked to
        // nothing would be worse than one that says it cannot connect.
        try (RaftClient client = new RaftClient(addresses)) {
            assertThrows(RaftClientException.class,
                    () -> client.submit("SET a 1".getBytes(StandardCharsets.UTF_8)));
        }
    }

    /** Answers every submit with OK, so the only thing that can fail is the transport. */
    private static final class EchoingClientService
            extends RaftClientServiceGrpc.RaftClientServiceImplBase {

        @Override
        public void submit(SubmitRequest request, StreamObserver<SubmitResponse> observer) {
            observer.onNext(SubmitResponse.newBuilder()
                    .setSuccess(true)
                    .setResult(ByteString.copyFrom("OK", StandardCharsets.UTF_8))
                    .build());
            observer.onCompleted();
        }

        @Override
        public void addServer(com.fbtberger.raft.client.proto.AddServerRequest request,
                              StreamObserver<ReconfigurationResponse> observer) {
            observer.onNext(ReconfigurationResponse.newBuilder().setSuccess(true).build());
            observer.onCompleted();
        }
    }
}
