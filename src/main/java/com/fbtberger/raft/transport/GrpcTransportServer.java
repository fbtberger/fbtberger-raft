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
                .addService(new RaftServiceAdapter(handler))
                .build();
    }

    public GrpcTransportServer(int port, RaftRpcHandler handler, TlsConfig tlsConfig) throws SSLException {
        if (!tlsConfig.enabled()) {
            this.server = ServerBuilder.forPort(port)
                    .addService(new RaftServiceAdapter(handler))
                    .build();
        } else {
            var sslBuilder = GrpcSslContexts.forServer(tlsConfig.certFile(), tlsConfig.keyFile())
                    .trustManager(tlsConfig.caFile());
            if (tlsConfig.mtlsEnabled()) {
                sslBuilder.clientAuth(ClientAuth.REQUIRE);
            }
            this.server = NettyServerBuilder.forPort(port)
                    .sslContext(sslBuilder.build())
                    .addService(new RaftServiceAdapter(handler))
                    .build();
        }
    }

    public GrpcTransportServer(ServerBuilder<?> builder, RaftRpcHandler handler) {
        this.server = builder.addService(new RaftServiceAdapter(handler)).build();
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

        public RaftServiceAdapter(RaftRpcHandler handler) { this.handler = handler; }

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
            try {
                observer.onNext(handler.handleRequestVote(request));
                observer.onCompleted();
            } catch (RuntimeException e) { fail("RequestVote", e, observer, ""); }
        }

        @Override
        public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> observer) {
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
            try {
                observer.onNext(handler.handleInstallSnapshot(request));
                observer.onCompleted();
            } catch (RuntimeException e) { fail("InstallSnapshot", e, observer, ""); }
        }

        @Override
        public void preVote(PreVoteRequest request, StreamObserver<PreVoteResponse> observer) {
            try {
                observer.onNext(handler.handlePreVote(request));
                observer.onCompleted();
            } catch (RuntimeException e) { fail("PreVote", e, observer, ""); }
        }

        @Override
        public void timeoutNow(TimeoutNowRequest request, StreamObserver<TimeoutNowResponse> observer) {
            try {
                observer.onNext(handler.handleTimeoutNow(request));
                observer.onCompleted();
            } catch (RuntimeException e) { fail("TimeoutNow", e, observer, ""); }
        }
    }
}
