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
import com.fbtberger.raft.proto.ReadIndexRequest;
import com.fbtberger.raft.proto.ReadIndexResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import com.fbtberger.raft.proto.RaftServiceGrpc;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import javax.net.ssl.SSLException;

public final class GrpcTransportServer implements RaftTransportServer {

    private final Server server;
    private final TlsConfig tlsConfig;

    public GrpcTransportServer(int port, RaftRpcHandler handler) {
        this.tlsConfig = TlsConfig.disabled();
        this.server = ServerBuilder.forPort(port)
                .addService(new RaftServiceAdapter(handler, false))
                .build();
    }

    public GrpcTransportServer(int port, RaftRpcHandler handler, TlsConfig tlsConfig) throws SSLException {
        this(port, handler, tlsConfig, List.of());
    }

    /**
     * As above, serving {@code additionalServices} on the same port and under the same
     * transport security as the peer service.
     *
     * <p>This is how the client service gets onto a TLS node. Before it existed the only
     * way to add a service was the {@link #GrpcTransportServer(ServerBuilder, RaftRpcHandler)}
     * constructor, which is plaintext by construction -- so the Spring wiring registered the
     * client service on a plaintext node and, with {@code tls.enabled=true}, on no node at
     * all.
     */
    public GrpcTransportServer(int port, RaftRpcHandler handler, TlsConfig tlsConfig,
                               List<ServerServiceDefinition> additionalServices) throws SSLException {
        this.tlsConfig = tlsConfig;
        if (!tlsConfig.enabled()) {
            ServerBuilder<?> builder = ServerBuilder.forPort(port)
                    .addService(new RaftServiceAdapter(handler, false));
            additionalServices.forEach(builder::addService);
            this.server = builder.build();
        } else {
            // The managers, not the files: a renewed certificate is then used by the next
            // handshake without this server being rebuilt. See TlsMaterial.
            //
            // The JDK engine, explicitly. gRPC would pick its bundled native one, and that
            // engine fixes the list of certificate authorities it names to a client when
            // the context is built. After a CA rollover the server would trust the new CA
            // and still ask clients for a certificate of the old one; a client holding
            // only the new one then sends none and is turned away with
            // CERTIFICATE_REQUIRED. The JDK engine asks the trust manager at every
            // handshake. TlsConfig makes the same choice for the Netty transport.
            var sslBuilder = GrpcSslContexts.configure(
                    io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder
                            .forServer(tlsConfig.material().keyManager()),
                    io.grpc.netty.shaded.io.netty.handler.ssl.SslProvider.JDK)
                    .trustManager(tlsConfig.material().trustManager());
            if (tlsConfig.mtlsEnabled()) {
                sslBuilder.clientAuth(ClientAuth.REQUIRE);
            }
            // The sender-id binding rides on mTLS and only on mTLS: without a client certificate
            // there is no authenticated identity to compare against, and demanding one would
            // reject every peer on a one-way-TLS cluster.
            NettyServerBuilder builder = NettyServerBuilder.forPort(port)
                    .sslContext(sslBuilder.build())
                    .intercept(new PeerIdentityInterceptor())
                    .addService(new RaftServiceAdapter(handler, tlsConfig.mtlsEnabled()));
            additionalServices.forEach(builder::addService);
            this.server = builder.build();
        }
    }

    public GrpcTransportServer(ServerBuilder<?> builder, RaftRpcHandler handler) {
        this.tlsConfig = TlsConfig.disabled();
        this.server = builder.addService(new RaftServiceAdapter(handler, false)).build();
    }

    @Override
    public void start() throws IOException {
        server.start();
        tlsConfig.logStartup("gRPC", server.getPort(), tlsConfig.mtlsEnabled());
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

            LOG.error("Raft RPC {} refused: the sender claims to be '{}', the certificate "
                            + "authenticates '{}'", rpc, claimedId, authenticated);
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
            LOG.error("Raft RPC {} failed{}", rpc,
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
         * Bound like the other four since {@code TimeoutNowRequest} carries a {@code leaderId}.
         * It used to carry a term and nothing else, which left this the one RPC any
         * cluster-certificate holder could send as anybody: TimeoutNow makes the receiver start
         * an election immediately (§3.10), so repeated calls were a cheap way to keep a cluster
         * churning.
         *
         * <p>A request without the field -- a peer that predates it -- claims the empty id, which
         * matches no certificate and is refused under mTLS. That is deliberate: accepting an
         * absent sender would let an attacker opt out of the check by omitting it. The cost is
         * that a leadership transfer from a not-yet-upgraded leader to an upgraded node fails
         * during a rolling upgrade of an mTLS cluster; nothing else depends on this RPC.
         *
         * <p>The binding says the sender is who it claims to be. Whether that node is entitled to
         * hand over the lead is the receiver's question, answered in
         * {@code RaftNode.handleTimeoutNow}.
         */
        /**
         * Bound to the certificate like the rest: the requester id decides whether the
         * leader answers at all, and an answer costs it a heartbeat round.
         */
        @Override
        public void readIndex(ReadIndexRequest request, StreamObserver<ReadIndexResponse> observer) {
            if (!senderIsWhoItClaims("ReadIndex", request.getRequesterId(), observer)) return;
            try {
                handler.handleReadIndex(request).whenComplete((response, failure) -> {
                    if (failure != null) {
                        fail("ReadIndex", failure instanceof RuntimeException r ? r : new RuntimeException(failure),
                                observer, "requester " + request.getRequesterId());
                    } else {
                        observer.onNext(response);
                        observer.onCompleted();
                    }
                });
            } catch (RuntimeException e) { fail("ReadIndex", e, observer, ""); }
        }

        @Override
        public void timeoutNow(TimeoutNowRequest request, StreamObserver<TimeoutNowResponse> observer) {
            if (!senderIsWhoItClaims("TimeoutNow", request.getLeaderId(), observer)) return;
            try {
                observer.onNext(handler.handleTimeoutNow(request));
                observer.onCompleted();
            } catch (RuntimeException e) { fail("TimeoutNow", e, observer, ""); }
        }
    }
}
