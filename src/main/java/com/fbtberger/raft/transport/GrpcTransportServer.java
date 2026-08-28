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
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import com.fbtberger.raft.proto.RaftServiceGrpc;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import javax.net.ssl.SSLException;

public final class GrpcTransportServer implements RaftTransportServer {

    private final Server server;

    public GrpcTransportServer(int port, RaftRpcHandler handler) {
        this.server = ServerBuilder.forPort(port)
                .addService(new RaftServiceAdapter(handler, false))
                .build();
    }

    public GrpcTransportServer(int port, RaftRpcHandler handler, TlsConfig tlsConfig) throws SSLException {
        if (!tlsConfig.enabled()) {
            this.server = ServerBuilder.forPort(port)
                    .addService(new RaftServiceAdapter(handler, false))
                    .build();
        } else {
            var sslBuilder = GrpcSslContexts.forServer(tlsConfig.certFile(), tlsConfig.keyFile())
                    .trustManager(tlsConfig.caFile());
            if (tlsConfig.mtlsEnabled()) {
                sslBuilder.clientAuth(ClientAuth.REQUIRE);
            }
            // The sender-id binding rides on mTLS and only on mTLS: without a client certificate
            // there is no authenticated identity to compare against, and demanding one would
            // reject every peer on a one-way-TLS cluster.
            this.server = NettyServerBuilder.forPort(port)
                    .sslContext(sslBuilder.build())
                    .intercept(new PeerIdentityInterceptor())
                    .addService(new RaftServiceAdapter(handler, tlsConfig.mtlsEnabled()))
                    .build();
        }
    }

    public GrpcTransportServer(ServerBuilder<?> builder, RaftRpcHandler handler) {
        this.server = builder.addService(new RaftServiceAdapter(handler, false)).build();
    }

    @Override
    public void start() throws IOException {
        server.start();
    }

    @Override
    public void close() {
        server.shutdown();
    }

    public Server grpcServer() { return server; }

    public static final class RaftServiceAdapter extends RaftServiceGrpc.RaftServiceImplBase {

        private static final Logger LOG = LoggerFactory.getLogger(GrpcTransportServer.class);

        private final RaftRpcHandler handler;
        private final boolean bindSenderToCertificate;

        public RaftServiceAdapter(RaftRpcHandler handler) { this(handler, false); }

        public RaftServiceAdapter(RaftRpcHandler handler, boolean bindSenderToCertificate) {
            this.handler = handler;
            this.bindSenderToCertificate = bindSenderToCertificate;
        }

        /**
         * Refuses the call unless the sender id in the message is the one the peer authenticated
         * as (§4.2, and the gap {@code GrpcMtlsRejectionTest} documents in its own class comment).
         *
         * <p>mTLS establishes that the caller holds a cluster certificate. It does not establish
         * <em>which</em> member it is, because every member's certificate is equally valid at the
         * transport. The sender id, meanwhile, is a self-declared string in the payload. Until the
         * two are compared, one compromised key — a learner's is enough — authenticates a
         * connection on which the holder may claim to be the leader, and a follower would accept
         * AppendEntries from it.
         *
         * <p>Returns true when the call may proceed; otherwise it has already been failed with
         * {@code PERMISSION_DENIED} and the caller must return immediately.
         *
         * <p>The refusal is deliberately noisy. A cluster that silently drops peer traffic looks
         * like a network fault, and the July 2026 experience is that peer-addressing failures cost
         * hours precisely because the rejecting side said nothing.
         */
        private <T> boolean senderIsWhoItClaims(String rpc, String claimedId, StreamObserver<T> observer) {
            if (!bindSenderToCertificate) return true;
            String authenticated = PeerIdentity.AUTHENTICATED_NODE_ID.get();
            if (authenticated != null && authenticated.equals(claimedId)) return true;

            LOG.error("Raft-RPC {} abgewiesen: Absender gibt sich als '{}' aus, "
                            + "authentisiert ist '{}'", rpc, claimedId, authenticated);
            observer.onError(io.grpc.Status.PERMISSION_DENIED
                    .withDescription("sender id does not match the authenticated certificate")
                    .asRuntimeException());
            return false;
        }

        /**
         * v102 — an exception in an RPC handler used to be answered with {@code observer.onError(e)}
         * and logged NOWHERE. gRPC turns it into a bare {@code UNKNOWN} on the caller's side, so the
         * leader reported "AppendEntries fehlgeschlagen: UNKNOWN" while the node that actually threw
         * said nothing at all. A follower can reject the entire log forever that way — the leader
         * resets nextIndex to 1, resends everything, the receiver throws again — with no way to find
         * out why. The receiving side is the only one that knows; it has to say so.
         */
        private static <T> void fail(String rpc, RuntimeException e,
                                     StreamObserver<T> observer, String context) {
            LOG.error("Raft-RPC {} fehlgeschlagen{}", rpc,
                    context.isEmpty() ? "" : " (" + context + ")", e);
            observer.onError(e);
        }

        @Override
        public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> observer) {
            if (!senderIsWhoItClaims("RequestVote", request.getCandidateId(), observer)) return;
            try {
                observer.onNext(handler.handleRequestVote(request));
                observer.onCompleted();
            } catch (RuntimeException e) { fail("RequestVote", e, observer, ""); }
        }

        @Override
        public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> observer) {
            if (!senderIsWhoItClaims("AppendEntries", request.getLeaderId(), observer)) return;
            try {
                observer.onNext(handler.handleAppendEntries(request));
                observer.onCompleted();
            } catch (RuntimeException e) {
                fail("AppendEntries", e, observer,
                        "prevLogIndex=" + request.getPrevLogIndex()
                                + " entries=" + request.getEntriesCount()
                                + " leaderCommit=" + request.getLeaderCommit()
                                + " term=" + request.getTerm());
            }
        }

        @Override
        public void installSnapshot(InstallSnapshotRequest request, StreamObserver<InstallSnapshotResponse> observer) {
            if (!senderIsWhoItClaims("InstallSnapshot", request.getLeaderId(), observer)) return;
            try {
                observer.onNext(handler.handleInstallSnapshot(request));
                observer.onCompleted();
            } catch (RuntimeException e) { fail("InstallSnapshot", e, observer, ""); }
        }

        @Override
        public void preVote(PreVoteRequest request, StreamObserver<PreVoteResponse> observer) {
            if (!senderIsWhoItClaims("PreVote", request.getCandidateId(), observer)) return;
            try {
                observer.onNext(handler.handlePreVote(request));
                observer.onCompleted();
            } catch (RuntimeException e) { fail("PreVote", e, observer, ""); }
        }

        /**
         * The one RPC that cannot be bound to its sender: {@code TimeoutNowRequest} carries a term
         * and nothing else — no {@code leaderId}, no {@code candidateId}. There is no claimed
         * identity to compare the certificate against, so the guard above does not apply here.
         *
         * <p>Stated rather than quietly skipped, because the omission is load-bearing. TimeoutNow
         * makes the receiver start an election immediately (§3.10), so any cluster-certificate
         * holder can provoke one, and repeated calls are a cheap way to keep a cluster churning —
         * a **D** vector that survives this change. Closing it needs a sender field in the proto,
         * which is a wire-format change and therefore its own deliberate step, not a side effect
         * of adding certificate binding. Everything an attacker gains is bounded by what a term
         * check already permits: a spurious election, not a forged log entry.
         */
        @Override
        public void timeoutNow(TimeoutNowRequest request, StreamObserver<TimeoutNowResponse> observer) {
            try {
                observer.onNext(handler.handleTimeoutNow(request));
                observer.onCompleted();
            } catch (RuntimeException e) { fail("TimeoutNow", e, observer, ""); }
        }
    }
}
