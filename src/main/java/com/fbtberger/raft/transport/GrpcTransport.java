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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ManagedChannel;

import java.util.concurrent.CompletableFuture;

public final class GrpcTransport implements RaftTransport {

    private final ManagedChannel channel;
    private final RaftServiceGrpc.RaftServiceFutureStub stub;

    public GrpcTransport(ManagedChannel channel) {
        this.channel = channel;
        this.stub = RaftServiceGrpc.newFutureStub(channel);
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return toCompletable(stub.requestVote(request));
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
        return toCompletable(stub.appendEntries(request));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        return toCompletable(stub.installSnapshot(request));
    }

    @Override
    public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest request) {
        return toCompletable(stub.preVote(request));
    }

    @Override
    public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest request) {
        return toCompletable(stub.timeoutNow(request));
    }

    /**
     * Abandons the channel's reconnect backoff so the next RPC attempts to connect right away
     * instead of waiting the backoff out (gRPC's default grows to seconds after a short outage).
     *
     * <p>{@code getState(true)} additionally asks an IDLE channel to start connecting, which
     * {@code resetConnectBackoff()} alone does not do.
     *
     * <p>Whether the underlying name resolution is refreshed too is deliberately NOT claimed here:
     * grpc-java refreshes it when the load balancer reports TRANSIENT_FAILURE, but that is an
     * implementation detail rather than an API guarantee. A peer that comes back on a NEW address
     * may therefore still need the caller's transport rebuild -- this method is the cheap first
     * attempt, not a replacement for it.
     */
    @Override
    public void resetBackoff() {
        channel.resetConnectBackoff();
        channel.getState(true);
    }

    @Override
    public void close() {
        channel.shutdownNow();
    }

    private static <T> CompletableFuture<T> toCompletable(ListenableFuture<T> lf) {
        CompletableFuture<T> cf = new CompletableFuture<>();
        Futures.addCallback(lf, new FutureCallback<T>() {
            @Override public void onSuccess(T result) { cf.complete(result); }
            @Override public void onFailure(Throwable t) { cf.completeExceptionally(t); }
        }, MoreExecutors.directExecutor());
        return cf;
    }
}
