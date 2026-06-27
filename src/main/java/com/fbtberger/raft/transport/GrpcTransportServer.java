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
        private final RaftRpcHandler handler;

        public RaftServiceAdapter(RaftRpcHandler handler) { this.handler = handler; }

        @Override
        public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> observer) {
            try {
                observer.onNext(handler.handleRequestVote(request));
                observer.onCompleted();
            } catch (RuntimeException e) { observer.onError(e); }
        }

        @Override
        public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> observer) {
            try {
                observer.onNext(handler.handleAppendEntries(request));
                observer.onCompleted();
            } catch (RuntimeException e) { observer.onError(e); }
        }

        @Override
        public void installSnapshot(InstallSnapshotRequest request, StreamObserver<InstallSnapshotResponse> observer) {
            try {
                observer.onNext(handler.handleInstallSnapshot(request));
                observer.onCompleted();
            } catch (RuntimeException e) { observer.onError(e); }
        }

        @Override
        public void preVote(PreVoteRequest request, StreamObserver<PreVoteResponse> observer) {
            try {
                observer.onNext(handler.handlePreVote(request));
                observer.onCompleted();
            } catch (RuntimeException e) { observer.onError(e); }
        }

        @Override
        public void timeoutNow(TimeoutNowRequest request, StreamObserver<TimeoutNowResponse> observer) {
            try {
                observer.onNext(handler.handleTimeoutNow(request));
                observer.onCompleted();
            } catch (RuntimeException e) { observer.onError(e); }
        }
    }
}
