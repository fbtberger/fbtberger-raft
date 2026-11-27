/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A node says at startup which transport security it actually came up with.
 *
 * <p>Until this existed a node logged nothing about TLS, so "is this cluster running mTLS?" had
 * no answer in the log, and a configuration that silently fell back to plaintext
 * ({@code tls.mtls.enabled=true} without {@code tls.enabled=true}) looked exactly like one that
 * worked. Each test here pins one of the things an operator has to be able to read off the log.
 */
class TlsStartupLoggingTest {

    private static final String NODE_ID = "kwatro-1";

    private Capture capture;
    private TestPki pki;
    private TestPki.Node node;

    @BeforeEach
    void setUp() throws Exception {
        capture = new Capture();
        capture.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        capture.start();
        logger().addAppender(capture);
        logger().setLevel(Level.INFO);
        pki = TestPki.create("kwatro-cluster-ca");
        node = pki.issue(NODE_ID);
    }

    @AfterEach
    void tearDown() throws Exception {
        logger().detachAppender(capture);
        pki.close();
    }

    @Test
    void plaintextIsAWarningNotSilence() {
        TlsConfig.disabled().logStartup("gRPC", 7001, false);

        assertTrue(capture.has(Level.WARN, "PLAINTEXT"), capture.toString());
        assertTrue(capture.has(Level.WARN, "port 7001"), capture.toString());
    }

    @Test
    void anMtlsFlagWithoutTlsIsReportedAsIgnored() {
        Properties props = new Properties();
        props.setProperty("tls.enabled", "false");
        props.setProperty("tls.mtls.enabled", "true");

        TlsConfig config = TlsConfig.fromProperties(props);

        assertFalse(config.enabled());
        assertTrue(capture.has(Level.WARN, "tls.mtls.enabled=true is IGNORED"), capture.toString());
    }

    @Test
    void noMtlsFlagMeansNoIgnoredWarning() {
        Properties props = new Properties();
        props.setProperty("tls.enabled", "false");

        TlsConfig.fromProperties(props);

        assertFalse(capture.has(Level.WARN, "IGNORED"), capture.toString());
    }

    @Test
    void mtlsNamesTheModeTheBindingAndTheCertificate() {
        node.tls(pki).logStartup("gRPC", 7001, true, Instant.now());

        assertTrue(capture.has(Level.INFO, "mTLS, client certificates required"), capture.toString());
        assertTrue(capture.has(Level.INFO, "sender id bound to the certificate CN"), capture.toString());
        assertTrue(capture.has(Level.INFO, "CN=" + NODE_ID), capture.toString());
        assertTrue(capture.has(Level.INFO, "valid until"), capture.toString());
    }

    @Test
    void aServerThatDoesNotBindSaysSo() {
        node.tls(pki).logStartup("Netty", 7001, false, Instant.now());

        assertTrue(capture.has(Level.INFO, "sender id NOT bound"), capture.toString());
    }

    @Test
    void oneWayTlsSaysPeersAreNotAuthenticated() {
        new TlsConfig(true, node.certFile(), node.keyFile(), pki.caFile(), false)
                .logStartup("gRPC", 7001, false, Instant.now());

        assertTrue(capture.has(Level.INFO, "server-authenticated only"), capture.toString());
        assertFalse(capture.has(Level.INFO, "mTLS"), capture.toString());
    }

    @Test
    void aCertificateCloseToExpiryIsWarnedAbout() {
        Instant notAfter = node.certificate().getNotAfter().toInstant();
        node.tls(pki).logStartup("gRPC", 7001, true, notAfter.minus(Duration.ofDays(3)));

        assertTrue(capture.has(Level.WARN, "expires at"), capture.toString());
    }

    @Test
    void aCertificateFarFromExpiryIsNotWarnedAbout() {
        Instant notAfter = node.certificate().getNotAfter().toInstant();
        node.tls(pki).logStartup("gRPC", 7001, true, notAfter.minus(Duration.ofDays(365)));

        assertFalse(capture.has(Level.WARN, "expires"), capture.toString());
        assertFalse(capture.has(Level.ERROR, "EXPIRED"), capture.toString());
    }

    @Test
    void anExpiredCertificateIsAnError() {
        Instant notAfter = node.certificate().getNotAfter().toInstant();
        node.tls(pki).logStartup("gRPC", 7001, true, notAfter.plusSeconds(1));

        assertTrue(capture.has(Level.ERROR, "EXPIRED"), capture.toString());
    }

    @Test
    void anUnreadableCertificateIsAWarningNotACrash() {
        new TlsConfig(true, new File("/nonexistent/cert.pem"), node.keyFile(), pki.caFile(), true)
                .logStartup("gRPC", 7001, true, Instant.now());

        assertTrue(capture.has(Level.INFO, "mTLS"), capture.toString());
        assertTrue(capture.has(Level.WARN, "could not read the node certificate"), capture.toString());
    }

    @Test
    void theGrpcServerLogsItsModeWhenItStarts() throws Exception {
        GrpcTransportServer server = new GrpcTransportServer(0, new NoopHandler(), node.tls(pki));
        try {
            server.start();
            int port = server.grpcServer().getPort();
            assertTrue(capture.has(Level.INFO, "(gRPC) on port " + port + ": mTLS"), capture.toString());
        } finally {
            server.close();
        }
    }

    @Test
    void theNettyServerLogsItsModeWhenItStarts() throws Exception {
        NettyTransportServer server = new NettyTransportServer(0, new NoopHandler(), node.tls(pki));
        try {
            server.start();
            assertTrue(capture.has(Level.INFO, "(Netty) on port"), capture.toString());
            assertTrue(capture.has(Level.INFO, ": mTLS"), capture.toString());
            assertTrue(capture.has(Level.INFO, "sender id bound to"), capture.toString());
            assertFalse(capture.has(Level.INFO, "on port 0:"), "logged the requested port, not the bound one");
        } finally {
            server.close();
        }
    }

    @Test
    void aPlaintextServerWarnsWhenItStarts() throws Exception {
        GrpcTransportServer server = new GrpcTransportServer(0, new NoopHandler());
        try {
            server.start();
            assertTrue(capture.has(Level.WARN, "(gRPC) on port " + server.grpcServer().getPort()
                    + ": PLAINTEXT"), capture.toString());
        } finally {
            server.close();
        }
    }

    private static ch.qos.logback.classic.Logger logger() {
        return (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(TlsConfig.class);
    }

    private static final class Capture extends AppenderBase<ILoggingEvent> {
        private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            events.add(event);
        }

        boolean has(Level level, String fragment) {
            return events.stream().anyMatch(e ->
                    e.getLevel() == level && e.getFormattedMessage().contains(fragment));
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("logged:");
            events.forEach(e -> sb.append("\n  ").append(e.getLevel()).append(' ')
                    .append(e.getFormattedMessage()));
            return sb.toString();
        }
    }

    private static final class NoopHandler implements RaftRpcHandler {
        @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest r) {
            return RequestVoteResponse.getDefaultInstance();
        }
        @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest r) {
            return AppendEntriesResponse.getDefaultInstance();
        }
        @Override public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest r) {
            return InstallSnapshotResponse.getDefaultInstance();
        }
        @Override public PreVoteResponse handlePreVote(PreVoteRequest r) {
            return PreVoteResponse.getDefaultInstance();
        }
        @Override public TimeoutNowResponse handleTimeoutNow(TimeoutNowRequest r) {
            return TimeoutNowResponse.getDefaultInstance();
        }
    }
}
