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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.File;
import java.math.BigInteger;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.cert.X509Certificate;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A certificate renewed on disk is used without a restart, and a renewal caught half-way
 * is not.
 *
 * <p>The node's three files live in a directory of their own and are replaced the way a
 * renewal tool replaces them. What the server presents is read off a real handshake.
 */
class TlsReloadTest {

    private static final String NODE = "kwatro-1";

    @TempDir
    Path live;

    private TestPki pki;
    private File cert;
    private File key;
    private File ca;
    private AutoCloseable server;

    @BeforeEach
    void setUp() throws Exception {
        pki = TestPki.create("kwatro-cluster-ca");
        cert = live.resolve("node.crt").toFile();
        key = live.resolve("node.key").toFile();
        ca = live.resolve("ca.crt").toFile();
        TestPki.Node first = pki.issue(NODE);
        copy(first.certFile(), cert);
        copy(first.keyFile(), key);
        copy(pki.caFile(), ca);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) server.close();
        pki.close();
    }

    // ---- the material itself ----------------------------------------------

    @Test
    void unchangedFilesAreNotReloaded() throws Exception {
        TlsMaterial material = new TlsMaterial(cert, key, ca);

        assertFalse(material.reloadIfChanged());
        assertEquals(0, material.reloadCount());
        assertEquals(0, material.failedReloadCount());
    }

    @Test
    void aRenewedCertificateIsTakenIntoUse() throws Exception {
        TlsMaterial material = new TlsMaterial(cert, key, ca);
        BigInteger before = material.certificate().getSerialNumber();

        TestPki.Node renewed = pki.issue(NODE);
        copy(renewed.certFile(), cert);
        copy(renewed.keyFile(), key);

        assertTrue(material.reloadIfChanged());
        assertEquals(renewed.certificate().getSerialNumber(), material.certificate().getSerialNumber());
        assertNotEquals(before, material.certificate().getSerialNumber());
        assertEquals(1, material.reloadCount());
    }

    @Test
    void aRenewalCaughtHalfWayIsRefusedAndCompletedLater() throws Exception {
        TlsMaterial material = new TlsMaterial(cert, key, ca);
        BigInteger before = material.certificate().getSerialNumber();
        TestPki.Node renewed = pki.issue(NODE);

        // The poll lands after the certificate was written and before the key was.
        copy(renewed.certFile(), cert);
        assertFalse(material.reloadIfChanged());
        assertEquals(before, material.certificate().getSerialNumber(),
                "a certificate without its key must not replace a working pair");
        assertEquals(1, material.failedReloadCount());

        // The same broken state seen again is not news.
        assertFalse(material.reloadIfChanged());
        assertEquals(1, material.failedReloadCount());

        copy(renewed.keyFile(), key);
        assertTrue(material.reloadIfChanged());
        assertEquals(renewed.certificate().getSerialNumber(), material.certificate().getSerialNumber());
    }

    @Test
    void aFileThatIsNotACertificateIsRefused() throws Exception {
        TlsMaterial material = new TlsMaterial(cert, key, ca);
        BigInteger before = material.certificate().getSerialNumber();

        Files.writeString(cert.toPath(), "-----BEGIN CERTIFICATE-----\nnot base64 at all\n");

        assertFalse(material.reloadIfChanged());
        assertEquals(before, material.certificate().getSerialNumber());
        assertEquals(1, material.failedReloadCount());
    }

    @Test
    void aNodeThatCannotReadItsCertificateAtStartDoesNotStart() throws Exception {
        Files.delete(key.toPath());

        assertThrows(Exception.class, () -> new TlsMaterial(cert, key, ca));
        assertThrows(javax.net.ssl.SSLException.class,
                () -> new TlsConfig(true, cert, key, ca, false).buildServerSslContext());
    }

    @Test
    void theIntervalComesFromThePropertiesAndZeroMeansNever() {
        Properties props = new Properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.cert.path", cert.getPath());
        props.setProperty("tls.key.path", key.getPath());
        props.setProperty("tls.ca.path", ca.getPath());
        assertEquals(TlsConfig.DEFAULT_RELOAD_INTERVAL_SECONDS,
                TlsConfig.fromProperties(props).reloadIntervalSeconds());

        props.setProperty("tls.reload.interval.seconds", "0");
        assertEquals(0, TlsConfig.fromProperties(props).reloadIntervalSeconds());

        props.setProperty("tls.reload.interval.seconds", "-5");
        assertThrows(IllegalArgumentException.class, () -> TlsConfig.fromProperties(props));
    }

    @Test
    void theWatcherPicksARenewalUpByItself() throws Exception {
        TlsConfig tls = new TlsConfig(true, cert, key, ca, false, /*every second*/ 1);
        TlsMaterial material = tls.material();

        TestPki.Node renewed = pki.issue(NODE);
        copy(renewed.certFile(), cert);
        copy(renewed.keyFile(), key);

        long deadline = System.currentTimeMillis() + 10_000;
        while (material.reloadCount() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        assertEquals(renewed.certificate().getSerialNumber(), material.certificate().getSerialNumber());
    }

    // ---- running servers ---------------------------------------------------

    @Test
    void aRunningGrpcServerPresentsTheRenewedCertificateAndKeepsItsConnections() throws Exception {
        TlsConfig tls = new TlsConfig(true, cert, key, ca, false, 0);
        int port = freePort();
        GrpcTransportServer grpc = new GrpcTransportServer(port, new Acknowledging(), tls);
        grpc.start();
        server = grpc;

        try (RaftTransport open = new GrpcTransportFactory(tls).connect("localhost:" + port)) {
            assertTrue(open.appendEntries(append()).get(10, TimeUnit.SECONDS).getSuccess());
            BigInteger before = presentedSerial(port, "h2");

            TestPki.Node renewed = pki.issue(NODE);
            copy(renewed.certFile(), cert);
            copy(renewed.keyFile(), key);
            assertTrue(tls.material().reloadIfChanged());

            assertEquals(renewed.certificate().getSerialNumber(), presentedSerial(port, "h2"),
                    "the next handshake must already see the renewed certificate");
            assertNotEquals(before, presentedSerial(port, "h2"));
            assertTrue(open.appendEntries(append()).get(10, TimeUnit.SECONDS).getSuccess(),
                    "the connection opened before the renewal is not dropped");
        }
    }

    @Test
    void aRunningNettyServerPresentsTheRenewedCertificate() throws Exception {
        TlsConfig tls = new TlsConfig(true, cert, key, ca, false, 0);
        int port = freePort();
        NettyTransportServer netty = new NettyTransportServer(port, new Acknowledging(), tls);
        netty.start();
        server = netty;
        BigInteger before = presentedSerial(port, null);

        TestPki.Node renewed = pki.issue(NODE);
        copy(renewed.certFile(), cert);
        copy(renewed.keyFile(), key);
        assertTrue(tls.material().reloadIfChanged());

        assertEquals(renewed.certificate().getSerialNumber(), presentedSerial(port, null));
        assertNotEquals(before, presentedSerial(port, null));
    }

    @Test
    void aRenewedCertificateAuthorityAdmitsPeersItSignedWithoutARestart() throws Exception {
        // mTLS this time: the server decides whom to let in by its CA file.
        TlsConfig tls = new TlsConfig(true, cert, key, ca, true, 0);
        int port = freePort();
        GrpcTransportServer grpc = new GrpcTransportServer(port, new Acknowledging(), tls);
        grpc.start();
        server = grpc;

        try (TestPki nextCa = TestPki.create("kwatro-cluster-ca-2027")) {
            TestPki.Node peer = nextCa.issue(NODE);
            TlsConfig peerTls = new TlsConfig(true, peer.certFile(), peer.keyFile(), pki.caFile(), true, 0);

            try (RaftTransport refused = new GrpcTransportFactory(peerTls).connect("localhost:" + port)) {
                assertThrows(ExecutionException.class,
                        () -> refused.appendEntries(append()).get(10, TimeUnit.SECONDS),
                        "control: a peer signed by an unknown CA is not let in");
            }

            // The rollover step of a CA change: trust both for a while.
            Files.write(ca.toPath(), (Files.readString(pki.caFile().toPath())
                    + Files.readString(nextCa.caFile().toPath())).getBytes());
            assertTrue(tls.material().reloadIfChanged());

            try (RaftTransport admitted = new GrpcTransportFactory(peerTls).connect("localhost:" + port)) {
                assertTrue(admitted.appendEntries(append()).get(10, TimeUnit.SECONDS).getSuccess());
            }
        }
    }

    // ---- helpers -----------------------------------------------------------

    /** The serial number of the certificate a server on {@code port} presents right now. */
    private static BigInteger presentedSerial(int port, String alpn) throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(null, new TrustManager[] {new X509TrustManager() {
            @Override public void checkClientTrusted(X509Certificate[] chain, String authType) { }
            @Override public void checkServerTrusted(X509Certificate[] chain, String authType) { }
            @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        }}, null);
        try (SSLSocket socket = (SSLSocket) context.getSocketFactory().createSocket("localhost", port)) {
            socket.setSoTimeout(10_000);
            if (alpn != null) {
                SSLParameters parameters = socket.getSSLParameters();
                parameters.setApplicationProtocols(new String[] {alpn});
                socket.setSSLParameters(parameters);
            }
            socket.startHandshake();
            return ((X509Certificate) socket.getSession().getPeerCertificates()[0]).getSerialNumber();
        }
    }

    private static void copy(File from, File to) throws Exception {
        Files.copy(from.toPath(), to.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static AppendEntriesRequest append() {
        return AppendEntriesRequest.newBuilder().setTerm(7).setLeaderId(NODE).build();
    }

    private static final class Acknowledging implements RaftRpcHandler {
        @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
            return RequestVoteResponse.getDefaultInstance();
        }
        @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
            return AppendEntriesResponse.newBuilder().setTerm(request.getTerm()).setSuccess(true).build();
        }
        @Override public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
            return InstallSnapshotResponse.getDefaultInstance();
        }
        @Override public PreVoteResponse handlePreVote(PreVoteRequest request) {
            return PreVoteResponse.getDefaultInstance();
        }
        @Override public TimeoutNowResponse handleTimeoutNow(TimeoutNowRequest request) {
            return TimeoutNowResponse.getDefaultInstance();
        }
    }
}
