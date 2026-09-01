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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.Context;
import io.grpc.ManagedChannel;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public final class GrpcTransport implements RaftTransport {

    private final ManagedChannel channel;
    private final RaftServiceGrpc.RaftServiceFutureStub stub;

    public GrpcTransport(ManagedChannel channel) {
        this.channel = channel;
        this.stub = RaftServiceGrpc.newFutureStub(channel);
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return detached(() -> stub.requestVote(request));
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
        return detached(() -> stub.appendEntries(request));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        return detached(() -> stub.installSnapshot(request));
    }

    @Override
    public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest request) {
        return detached(() -> stub.preVote(request));
    }

    @Override
    public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest request) {
        return detached(() -> stub.timeoutNow(request));
    }

    @Override
    public void close() {
        channel.shutdownNow();
    }

    /**
     * Issues the call outside whatever gRPC context happens to be current.
     *
     * <p>gRPC propagates an inbound call's {@link Context} to outbound calls made on the same
     * thread, and cancels them with it. A Raft node dispatches peer RPCs from inside other
     * RPCs' completion handlers -- {@code checkTransferReadyLocked} sends TimeoutNow from the
     * AppendEntries response callback, replication pipelines the next batch from the previous
     * one's -- so the outbound call inherits a context that is about to end, and dies with it
     * for no reason connected to the peer.
     *
     * <p>Measured, and it is why a leadership transfer failed under load and worked when idle:
     * with the target already level, TimeoutNow goes out on the caller's own thread and
     * survives; with the target trailing it goes out later, from a replication callback, and
     * comes back "CANCELLED: io.grpc.Context was cancelled without error" while the leader
     * still leads. Two rehearsals out of three. The same error had been in the node logs for
     * days against AppendEntries, where it is invisible because replication simply retries.
     *
     * <p>{@code Context.ROOT} is never cancelled. No deadline is lost by leaving the inherited
     * context: peer RPC timeouts are a decorator, {@link TimeoutTransport}, not context state.
     */
    private static <T> CompletableFuture<T> detached(Supplier<ListenableFuture<T>> call) {
        try {
            return toCompletable(Context.ROOT.call(call::get));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
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
