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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * The sender id in a Raft message must be the id the peer authenticated as.
 *
 * <p>{@code GrpcMtlsRejectionTest} proves membership of the cluster PKI and says so in its own
 * class comment: <em>"A certificate signed by the cluster CA is accepted for any role, so a
 * compromised learner key can still open a connection and claim leaderId=kwatro-1 in the payload
 * … Closing that gap is a separate change"</em>. This is that change, and this is the test that
 * holds it shut.
 *
 * <p>The distinction matters because the two failures look identical from outside and are not:
 * mTLS answers "is this one of us", the check under test answers "is this the one it says it is".
 * Without the second, one compromised key — a learner's will do, and learners are the least
 * guarded nodes — authenticates a connection on which the holder may present as the leader, and a
 * follower will take AppendEntries from it. That is the impersonation the whole control exists to
 * prevent.
 *
 * <h2>Why a real CA and real node certificates</h2>
 * Both peers below hold certificates issued by the <em>same</em> cluster CA, so the transport
 * trusts each of them completely: a rejection here cannot be a trust failure and can only be the
 * identity check firing. That is the isolation this test needs, and it is why the setup is a
 * genuine PKI rather than a pair of self-signed certificates.
 *
 * <p>It also caught a design error that a simpler setup hid. A node certificate has to carry the
 * node id in its CN <em>and</em> the address in a SAN: TLS verifies the peer certificate against
 * the host the client dialled, and peers are dialled by IP or Compose service name, never by node
 * id. The first version of this test used {@code CN=kwatro1} with no SAN, dialled
 * {@code localhost}, and died in {@code HostnameChecker} — which is exactly what production would
 * have done on the first handshake. See {@link TestPki}.
 */
class GrpcPeerIdentityBindingTest {

    private TestPki pki;
    private TestPki.Node kwatro1;
    private TestPki.Node kwatro2;
    private CountingHandler handler;
    private GrpcTransportServer server;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        pki = TestPki.create("kwatro-cluster-ca");
        kwatro1 = pki.issue("kwatro1");
        kwatro2 = pki.issue("kwatro2");
        handler = new CountingHandler();
        try (ServerSocket ss = new ServerSocket(0)) {
            port = ss.getLocalPort();
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) server.close();
        if (pki != null) pki.close();
    }

    /** A server as kwatro1, trusting the cluster CA — so BOTH node certificates are valid to it. */
    private void startServer() throws Exception {
        server = new GrpcTransportServer(port, handler, kwatro1.tls(pki));
        server.start();
    }

    /** A client presenting {@code identity}'s certificate. Trusted at the transport, every time. */
    private RaftTransport clientAs(TestPki.Node identity) {
        return new GrpcTransportFactory(identity.tls(pki)).connect("localhost:" + port);
    }

    /**
     * The control group. Without it every assertion below would stay green on a server that
     * rejects everything — a broken handshake, a wrong port, a certificate the trust manager
     * never accepts. This is what makes the rejections mean "the identity check fired".
     */
    @Test
    @DisplayName("control: kwatro2 claiming to be kwatro2 is accepted")
    void acceptsAMatchingSenderId() throws Exception {
        startServer();
        try (RaftTransport client = clientAs(kwatro2)) {
            AppendEntriesResponse resp =
                    client.appendEntries(appendEntriesFrom("kwatro2")).get(10, TimeUnit.SECONDS);
            assertTrue(resp.getSuccess(), "a peer that told the truth was rejected");
            assertEquals(1, handler.appendEntries.get(), "handler should have seen the call");
        }
    }

    /** THE POINT: an authentic connection, a forged sender. */
    @Test
    @DisplayName("kwatro2 claiming to be kwatro1 is refused, though its certificate is trusted")
    void rejectsAForgedLeaderId() throws Exception {
        startServer();
        try (RaftTransport client = clientAs(kwatro2)) {
            assertRejected(() -> client.appendEntries(appendEntriesFrom("kwatro1")));
            assertEquals(0, handler.appendEntries.get(),
                    "the forged AppendEntries reached the handler -- the binding did not hold");
        }
    }

    @Test
    @DisplayName("a forged candidateId is refused on RequestVote")
    void rejectsAForgedCandidateIdOnRequestVote() throws Exception {
        startServer();
        try (RaftTransport client = clientAs(kwatro2)) {
            assertRejected(() -> client.requestVote(RequestVoteRequest.newBuilder()
                    .setTerm(2).setCandidateId("kwatro1").build()));
            assertEquals(0, handler.requestVote.get());
        }
    }

    @Test
    @DisplayName("a forged candidateId is refused on PreVote")
    void rejectsAForgedCandidateIdOnPreVote() throws Exception {
        startServer();
        try (RaftTransport client = clientAs(kwatro2)) {
            assertRejected(() -> client.preVote(PreVoteRequest.newBuilder()
                    .setTerm(2).setCandidateId("kwatro1").build()));
            assertEquals(0, handler.preVote.get());
        }
    }

    /**
     * InstallSnapshot ships state, not just a heartbeat — a forged one hands a follower a whole
     * state machine. Covered separately from AppendEntries for that reason.
     */
    @Test
    @DisplayName("a forged leaderId is refused on InstallSnapshot")
    void rejectsAForgedLeaderIdOnInstallSnapshot() throws Exception {
        startServer();
        try (RaftTransport client = clientAs(kwatro2)) {
            assertRejected(() -> client.installSnapshot(InstallSnapshotRequest.newBuilder()
                    .setTerm(1).setLeaderId("kwatro1").setDone(true).build()));
            assertEquals(0, handler.installSnapshot.get());
        }
    }

    /**
     * The documented hole, pinned so it cannot close by accident and cannot widen unnoticed.
     * TimeoutNowRequest carries no sender field, so there is nothing to bind; the call goes
     * through on any trusted certificate. If a sender id is ever added to the proto, this test
     * fails and the guard belongs on that RPC too.
     */
    @Test
    @DisplayName("TimeoutNow cannot be bound -- it carries no sender id at all")
    void timeoutNowIsNotBoundBecauseItCarriesNoSender() throws Exception {
        startServer();
        assertEquals(1, TimeoutNowRequest.getDescriptor().getFields().size(),
                "TimeoutNowRequest gained a field -- if it is a sender id, bind it (see "
                        + "GrpcTransportServer.timeoutNow)");
        try (RaftTransport client = clientAs(kwatro2)) {
            client.timeoutNow(TimeoutNowRequest.newBuilder().setTerm(2).build())
                    .get(10, TimeUnit.SECONDS);
            assertEquals(1, handler.timeoutNow.get());
        }
    }

    /** Without mTLS nothing is bound, and a cluster with TLS off keeps working unchanged. */
    @Test
    @DisplayName("with mTLS off the sender id is not checked")
    void plaintextClusterIsUnaffected() throws Exception {
        server = new GrpcTransportServer(port, handler);
        server.start();
        try (RaftTransport client = new GrpcTransportFactory().connect("localhost:" + port)) {
            AppendEntriesResponse resp =
                    client.appendEntries(appendEntriesFrom("anything-at-all")).get(10, TimeUnit.SECONDS);
            assertTrue(resp.getSuccess());
            assertEquals(1, handler.appendEntries.get());
        }
    }

    private interface Call {
        java.util.concurrent.Future<?> run();
    }

    private void assertRejected(Call call) {
        try {
            call.run().get(10, TimeUnit.SECONDS);
            fail("the call was accepted; the sender-id binding did not fire");
        } catch (ExecutionException e) {
            assertTrue(String.valueOf(e.getCause()).contains("PERMISSION_DENIED"),
                    "expected PERMISSION_DENIED, got: " + e.getCause());
        } catch (Exception e) {
            fail("unexpected failure: " + e);
        }
    }

    private static AppendEntriesRequest appendEntriesFrom(String leaderId) {
        return AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId(leaderId)
                .setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0)
                .build();
    }

    /** Counts what actually reached the handler — a rejection must leave every counter at zero. */
    private static final class CountingHandler implements RaftRpcHandler {
        final AtomicInteger appendEntries = new AtomicInteger();
        final AtomicInteger requestVote = new AtomicInteger();
        final AtomicInteger preVote = new AtomicInteger();
        final AtomicInteger installSnapshot = new AtomicInteger();
        final AtomicInteger timeoutNow = new AtomicInteger();

        @Override
        public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
            appendEntries.incrementAndGet();
            return AppendEntriesResponse.newBuilder().setTerm(request.getTerm()).setSuccess(true).build();
        }

        @Override
        public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
            requestVote.incrementAndGet();
            return RequestVoteResponse.newBuilder().setTerm(request.getTerm()).setVoteGranted(true).build();
        }

        @Override
        public PreVoteResponse handlePreVote(PreVoteRequest request) {
            preVote.incrementAndGet();
            return PreVoteResponse.newBuilder().setTerm(request.getTerm()).setVoteGranted(true).build();
        }

        @Override
        public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
            installSnapshot.incrementAndGet();
            return InstallSnapshotResponse.newBuilder().setTerm(request.getTerm()).build();
        }

        @Override
        public TimeoutNowResponse handleTimeoutNow(TimeoutNowRequest request) {
            timeoutNow.incrementAndGet();
            return TimeoutNowResponse.newBuilder().build();
        }
    }
}
