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
import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Every future {@link NettyTransport} hands out completes, whatever happens to the connection.
 *
 * <p>The defect these pin: a request's future was completed by its response or by an exception in
 * the pipeline, and by nothing else. A peer that simply closes the connection -- because it
 * rejected the client certificate, could not parse the frame, had its handler throw, or died --
 * produces neither, so the request stayed pending forever. Each test sets up one of those ways a
 * connection ends and requires the future to fail <em>quickly</em>, well inside any RPC timeout:
 * failing after {@link TimeoutTransport} gives up is precisely the symptom, not the fix.
 *
 * <p>The peers here are mostly raw sockets rather than a {@link NettyTransportServer}, because
 * the point is to control exactly what the other side does -- read and hang up, never answer,
 * answer with garbage -- which a well-behaved server will not do on request.
 */
class NettyTransportCompletesEveryFutureTest {

    /** Far below any configured RPC timeout; a correct transport fails in milliseconds. */
    private static final long FAIL_FAST_SECONDS = 5;

    private NioEventLoopGroup clientGroup;
    private final List<AutoCloseable> cleanup = new ArrayList<>();

    @BeforeEach
    void setUp() {
        clientGroup = new NioEventLoopGroup(1);
    }

    @AfterEach
    void tearDown() throws Exception {
        for (int i = cleanup.size() - 1; i >= 0; i--) {
            try {
                cleanup.get(i).close();
            } catch (Exception ignored) {
                // best effort
            }
        }
        clientGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
    }

    @Test
    void aPeerThatReadsTheRequestAndHangsUpFailsTheRequest() throws Exception {
        RawPeer peer = rawPeer(socket -> {
            readFrame(new DataInputStream(socket.getInputStream()));
            socket.close();
        });
        NettyTransport client = connect(peer.port(), null);

        assertFailsFast(client.requestVote(vote()));
        assertEquals(0, client.pendingCount(), "a failed request stayed in the pending map");
    }

    @Test
    void everyRequestInFlightFailsWhenThePeerHangsUp() throws Exception {
        CountDownLatch threeRead = new CountDownLatch(1);
        RawPeer peer = rawPeer(socket -> {
            DataInputStream in = new DataInputStream(socket.getInputStream());
            for (int i = 0; i < 3; i++) readFrame(in);
            threeRead.await(FAIL_FAST_SECONDS, TimeUnit.SECONDS);
            socket.close();
        });
        NettyTransport client = connect(peer.port(), null);

        CompletableFuture<RequestVoteResponse> a = client.requestVote(vote());
        CompletableFuture<AppendEntriesResponse> b = client.appendEntries(append());
        CompletableFuture<PreVoteResponse> c = client.preVote(PreVoteRequest.getDefaultInstance());
        threeRead.countDown();

        assertFailsFast(a);
        assertFailsFast(b);
        assertFailsFast(c);
        assertEquals(0, client.pendingCount());
    }

    @Test
    void aRequestAfterThePeerHungUpFailsImmediately() throws Exception {
        RawPeer peer = rawPeer(Socket::close);
        NettyTransport client = connect(peer.port(), null);
        assertFailsFast(client.requestVote(vote()));

        CompletableFuture<AppendEntriesResponse> later = client.appendEntries(append());

        assertTrue(later.isCompletedExceptionally(), "a request on a dead connection was not failed at once");
        assertEquals(0, client.pendingCount());
    }

    @Test
    void closeFailsWhatIsInFlightAndEverythingAfter() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        // Reads, never answers, keeps the connection open: the request can only end by close().
        RawPeer peer = rawPeer(socket -> {
            readFrame(new DataInputStream(socket.getInputStream()));
            release.await(30, TimeUnit.SECONDS);
        });
        cleanup.add(release::countDown);
        NettyTransport client = connect(peer.port(), null);

        CompletableFuture<RequestVoteResponse> inFlight = client.requestVote(vote());
        client.close();

        assertFailsFast(inFlight);
        assertTrue(client.installSnapshot(InstallSnapshotRequest.getDefaultInstance())
                .isCompletedExceptionally());
        assertEquals(0, client.pendingCount());
    }

    @Test
    void aResponseOfAnUnknownTypeFailsItsOwnRequest() throws Exception {
        // The response is read and taken out of the pending map before it is decoded, so if the
        // decode throws anything other than InvalidProtocolBufferException the future is reachable
        // from nowhere. The connection stays open on purpose: nothing else would rescue it.
        CountDownLatch release = new CountDownLatch(1);
        RawPeer peer = rawPeer(socket -> {
            int reqId = readFrame(new DataInputStream(socket.getInputStream()));
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(NettyProtocol.HEADER_BYTES);
            out.writeByte(99);
            out.writeInt(reqId);
            out.flush();
            release.await(30, TimeUnit.SECONDS);
        });
        cleanup.add(release::countDown);
        NettyTransport client = connect(peer.port(), null);

        assertFailsFast(client.requestVote(vote()));
        assertEquals(0, client.pendingCount());
    }

    @Test
    void aHandlerThatThrowsOnTheServerFailsTheRequestOnTheClient() throws Exception {
        // NettyTransportServer has no error frame: when the handler throws, the server closes the
        // connection. That is only acceptable if the client notices the close.
        NettyTransportServer server = new NettyTransportServer(0, new ThrowingHandler());
        server.start();
        cleanup.add(server::close);
        NettyTransport client = connect(boundPort(server), null);

        assertFailsFast(client.appendEntries(append()));
        assertEquals(0, client.pendingCount());
    }

    @Test
    void anMtlsServerThatRejectsTheClientFailsTheRequest() throws Exception {
        // Under TLS 1.3 the client's side of the handshake finishes before the server has looked
        // at the client certificate, so connect() succeeds and the rejection arrives afterwards as
        // a closed connection -- the exact shape the old transport never noticed.
        try (TestPki pki = TestPki.create("kwatro-cluster-ca")) {
            TestPki.Node node = pki.issue("kwatro-1");
            NettyTransportServer server = new NettyTransportServer(0, new ThrowingHandler(), node.tls(pki));
            server.start();
            cleanup.add(server::close);
            SslContext noClientCert = SslContextBuilder.forClient()
                    .sslProvider(SslProvider.JDK)
                    .trustManager(pki.caFile())
                    .build();

            NettyTransport client;
            try {
                client = connect(boundPort(server), noClientCert);
            } catch (RuntimeException rejectedDuringConnect) {
                return; // TLS 1.2 style: refused before there was anything to hang
            }

            assertFailsFast(client.requestVote(vote()));
            assertEquals(0, client.pendingCount());
        }
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static void assertFailsFast(CompletableFuture<?> future) throws Exception {
        try {
            future.get(FAIL_FAST_SECONDS, TimeUnit.SECONDS);
            fail("the request succeeded against a peer that cannot have answered it");
        } catch (TimeoutException e) {
            fail("the request was still pending after " + FAIL_FAST_SECONDS + "s: it hangs");
        } catch (ExecutionException expected) {
            assertInstanceOf(Throwable.class, expected.getCause());
        }
        assertThrows(ExecutionException.class, future::get);
    }

    private NettyTransport connect(int port, SslContext ssl) {
        NettyTransport client = new NettyTransport("localhost", port, clientGroup, ssl);
        cleanup.add(client::close);
        return client;
    }

    private static int boundPort(NettyTransportServer server) {
        return server.boundPort();
    }

    /** Reads one length-prefixed frame and returns its request id. */
    private static int readFrame(DataInputStream in) throws Exception {
        int len = in.readInt();
        in.readByte();
        int reqId = in.readInt();
        in.readFully(new byte[len - NettyProtocol.HEADER_BYTES]);
        return reqId;
    }

    private static RequestVoteRequest vote() {
        return RequestVoteRequest.newBuilder().setTerm(1).setCandidateId("kwatro-1").build();
    }

    private static AppendEntriesRequest append() {
        return AppendEntriesRequest.newBuilder().setTerm(1).setLeaderId("kwatro-1").build();
    }

    @FunctionalInterface
    private interface Behaviour {
        void on(Socket socket) throws Exception;
    }

    private record RawPeer(int port) { }

    /** A peer that accepts one connection and does exactly what {@code behaviour} says. */
    private RawPeer rawPeer(Behaviour behaviour) throws Exception {
        ServerSocket ss = new ServerSocket(0);
        cleanup.add(ss);
        Thread t = new Thread(() -> {
            try (Socket socket = ss.accept()) {
                behaviour.on(socket);
            } catch (Exception ignored) {
                // the test asserts on the client, not on this side
            }
        }, "raw-peer");
        t.setDaemon(true);
        t.start();
        return new RawPeer(ss.getLocalPort());
    }

    private static final class ThrowingHandler implements RaftRpcHandler {
        @Override public RequestVoteResponse handleRequestVote(RequestVoteRequest r) {
            throw new IllegalStateException("handler failure");
        }
        @Override public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest r) {
            throw new IllegalStateException("handler failure");
        }
        @Override public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest r) {
            throw new IllegalStateException("handler failure");
        }
        @Override public PreVoteResponse handlePreVote(PreVoteRequest r) {
            throw new IllegalStateException("handler failure");
        }
        @Override public TimeoutNowResponse handleTimeoutNow(TimeoutNowRequest r) {
            throw new IllegalStateException("handler failure");
        }
    }
}
