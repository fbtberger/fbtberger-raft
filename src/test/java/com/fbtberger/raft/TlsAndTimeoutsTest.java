/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.*;
import com.fbtberger.raft.transport.*;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class TlsAndTimeoutsTest {

    @Test
    void tlsDisabledByDefault() {
        TlsConfig tls = TlsConfig.fromProperties(new Properties());
        assertFalse(tls.enabled());
    }

    @Test
    void tlsDisabledExplicitly() {
        Properties props = new Properties();
        props.setProperty("tls.enabled", "false");
        TlsConfig tls = TlsConfig.fromProperties(props);
        assertFalse(tls.enabled());
    }

    @Test
    void tlsEnabledRequiresCertPaths() {
        Properties props = new Properties();
        props.setProperty("tls.enabled", "true");
        assertThrows(IllegalArgumentException.class, () -> TlsConfig.fromProperties(props));
    }

    @Test
    void tlsEnabledParsesAllPaths() {
        Properties props = new Properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.cert.path", "/tmp/cert.pem");
        props.setProperty("tls.key.path", "/tmp/key.pem");
        props.setProperty("tls.ca.path", "/tmp/ca.pem");
        props.setProperty("tls.mtls.enabled", "true");
        TlsConfig tls = TlsConfig.fromProperties(props);
        assertTrue(tls.enabled());
        assertTrue(tls.mtlsEnabled());
        assertEquals("/tmp/cert.pem", tls.certFile().getPath());
    }

    @Test
    void tlsDisabledFactory() {
        TlsConfig tls = TlsConfig.disabled();
        assertFalse(tls.enabled());
        assertFalse(tls.mtlsEnabled());
    }

    @Test
    void rpcTimeoutsDefaults() {
        RpcTimeouts t = RpcTimeouts.defaults();
        assertEquals(1000, t.requestVoteMs());
        assertEquals(2000, t.appendEntriesMs());
        assertEquals(30000, t.installSnapshotMs());
        assertEquals(1000, t.preVoteMs());
    }

    @Test
    void rpcTimeoutsFromProperties() {
        Properties props = new Properties();
        props.setProperty("rpc.timeout.request.vote.ms", "500");
        props.setProperty("rpc.timeout.append.entries.ms", "750");
        props.setProperty("rpc.timeout.install.snapshot.ms", "5000");
        props.setProperty("rpc.timeout.pre.vote.ms", "600");
        RpcTimeouts t = RpcTimeouts.fromProperties(props);
        assertEquals(500, t.requestVoteMs());
        assertEquals(750, t.appendEntriesMs());
        assertEquals(5000, t.installSnapshotMs());
        assertEquals(600, t.preVoteMs());
    }

    @Test
    void rpcTimeoutsFallsBackToDefaults() {
        RpcTimeouts t = RpcTimeouts.fromProperties(new Properties());
        assertEquals(RpcTimeouts.DEFAULT_REQUEST_VOTE_MS, t.requestVoteMs());
        assertEquals(RpcTimeouts.DEFAULT_APPEND_ENTRIES_MS, t.appendEntriesMs());
    }

    @Test
    void nettyTlsRoundTrip() throws Exception {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        TlsConfig tlsConfig = new TlsConfig(true,
                ssc.certificate(), ssc.privateKey(), ssc.certificate(), false);

        int port;
        try (java.net.ServerSocket ss = new java.net.ServerSocket(0)) { port = ss.getLocalPort(); }

        StubHandler handler = new StubHandler();
        com.fbtberger.raft.transport.NettyTransportServer server =
                new com.fbtberger.raft.transport.NettyTransportServer(port, handler, tlsConfig);
        server.start();
        try {
            io.netty.handler.ssl.SslContext clientSsl = tlsConfig.buildClientSslContext();
            io.netty.channel.nio.NioEventLoopGroup group = new io.netty.channel.nio.NioEventLoopGroup(1);
            com.fbtberger.raft.transport.NettyTransport client =
                    new com.fbtberger.raft.transport.NettyTransport("localhost", port, group, clientSsl);
            try {
                RequestVoteResponse resp = client.requestVote(
                        RequestVoteRequest.newBuilder()
                                .setTerm(5).setCandidateId("n1")
                                .setLastLogIndex(1).setLastLogTerm(1).build()
                ).get(5, TimeUnit.SECONDS);
                assertEquals(5, resp.getTerm());
                assertTrue(resp.getVoteGranted());
            } finally {
                client.close();
                group.shutdownGracefully().sync();
            }
        } finally {
            server.close();
        }
    }

    @Test
    void grpcTlsRoundTrip() throws Exception {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        TlsConfig tlsConfig = new TlsConfig(true,
                ssc.certificate(), ssc.privateKey(), ssc.certificate(), false);

        int port;
        try (ServerSocket ss = new ServerSocket(0)) { port = ss.getLocalPort(); }

        StubHandler handler = new StubHandler();
        GrpcTransportServer server = new GrpcTransportServer(port, handler, tlsConfig);
        server.start();
        try {
            GrpcTransportFactory factory = new GrpcTransportFactory(tlsConfig);
            RaftTransport client = factory.connect("localhost:" + port);
            try {
                RequestVoteResponse resp = client.requestVote(
                        RequestVoteRequest.newBuilder()
                                .setTerm(3).setCandidateId("n1")
                                .setLastLogIndex(1).setLastLogTerm(1).build()
                ).get(5, TimeUnit.SECONDS);
                assertEquals(3, resp.getTerm());
                assertTrue(resp.getVoteGranted());
            } finally {
                client.close();
            }
        } finally {
            server.close();
        }
    }

    @Test
    void grpcMtlsRoundTrip() throws Exception {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        TlsConfig tlsConfig = new TlsConfig(true,
                ssc.certificate(), ssc.privateKey(), ssc.certificate(), true);

        int port;
        try (ServerSocket ss = new ServerSocket(0)) { port = ss.getLocalPort(); }

        StubHandler handler = new StubHandler();
        GrpcTransportServer server = new GrpcTransportServer(port, handler, tlsConfig);
        server.start();
        try {
            GrpcTransportFactory factory = new GrpcTransportFactory(tlsConfig);
            RaftTransport client = factory.connect("localhost:" + port);
            try {
                AppendEntriesResponse resp = client.appendEntries(
                        AppendEntriesRequest.newBuilder()
                                .setTerm(2).setLeaderId("leader")
                                .setPrevLogIndex(0).setPrevLogTerm(0).build()
                ).get(5, TimeUnit.SECONDS);
                assertEquals(2, resp.getTerm());
                assertTrue(resp.getSuccess());
            } finally {
                client.close();
            }
        } finally {
            server.close();
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
