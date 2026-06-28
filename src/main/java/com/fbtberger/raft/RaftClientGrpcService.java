/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.client.proto.AddServerRequest;
import com.fbtberger.raft.client.proto.RaftClientServiceGrpc;
import com.fbtberger.raft.client.proto.ReconfigurationResponse;
import com.fbtberger.raft.client.proto.RemoveServerRequest;
import com.fbtberger.raft.client.proto.SubmitRequest;
import com.fbtberger.raft.client.proto.SubmitResponse;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.CompletionException;

/**
 * Adapter between the client-facing {@code RaftClientService} (client.proto) and
 * {@link RaftNode}. This class has no Raft logic of its own: it forwards each request to the matching
 * {@link RaftNode} method ({@link RaftNode#submitCommand}, {@link RaftNode#addServer},
 * {@link RaftNode#removeServer}), waits for the result, and translates the outcome
 * into the corresponding response message -- success, or failure with a leader hint
 * when this node isn't the leader (paper, §8 and §6).
 */
public final class RaftClientGrpcService extends RaftClientServiceGrpc.RaftClientServiceImplBase {

    private final RaftNode raftNode;
    private final RaftMetrics metrics;

    public RaftClientGrpcService(RaftNode raftNode) {
        this.raftNode = raftNode;
        this.metrics = raftNode.metrics();
    }

    @Override
    public void submit(SubmitRequest request, StreamObserver<SubmitResponse> responseObserver) {
        Timer.Sample sample = Timer.start(metrics.registry());
        raftNode.submitCommand(request.getCommand().toByteArray()).whenComplete((result, throwable) -> {
            sample.stop(metrics.clientSubmitTimer());
            responseObserver.onNext(throwable == null ? success(result) : failure(unwrap(throwable)));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void addServer(AddServerRequest request, StreamObserver<ReconfigurationResponse> responseObserver) {
        raftNode.addServer(request.getId(), request.getAddress()).whenComplete((result, throwable) -> {
            responseObserver.onNext(throwable == null ? reconfigurationSuccess() : reconfigurationFailure(unwrap(throwable)));
            responseObserver.onCompleted();
        });
    }

    @Override
    public void removeServer(RemoveServerRequest request, StreamObserver<ReconfigurationResponse> responseObserver) {
        raftNode.removeServer(request.getId()).whenComplete((result, throwable) -> {
            responseObserver.onNext(throwable == null ? reconfigurationSuccess() : reconfigurationFailure(unwrap(throwable)));
            responseObserver.onCompleted();
        });
    }

    private static SubmitResponse success(byte[] result) {
        return SubmitResponse.newBuilder()
                .setSuccess(true)
                .setResult(ByteString.copyFrom(result))
                .build();
    }

    private static SubmitResponse failure(Throwable cause) {
        SubmitResponse.Builder builder = SubmitResponse.newBuilder().setSuccess(false);
        if (cause instanceof RaftNode.NotLeaderException nle) {
            if (nle.leaderHint != null) {
                builder.setLeaderHint(nle.leaderHint);
            }
            builder.setError("not leader");
        } else {
            builder.setError(cause.getMessage() != null ? cause.getMessage() : cause.toString());
        }
        return builder.build();
    }

    private static ReconfigurationResponse reconfigurationSuccess() {
        return ReconfigurationResponse.newBuilder().setSuccess(true).build();
    }

    private static ReconfigurationResponse reconfigurationFailure(Throwable cause) {
        ReconfigurationResponse.Builder builder = ReconfigurationResponse.newBuilder().setSuccess(false);
        if (cause instanceof RaftNode.NotLeaderException nle) {
            if (nle.leaderHint != null) {
                builder.setLeaderHint(nle.leaderHint);
            }
            builder.setError("not leader");
        } else {
            // Covers RaftNode.ConfigurationChangeException (already-a-member,
            // not-a-member, last-member, change-in-flight) -- none of those
            // come with a leader hint, just a human-readable reason.
            builder.setError(cause.getMessage() != null ? cause.getMessage() : cause.toString());
        }
        return builder.build();
    }

    // CompletableFuture.whenComplete hands back the raw exception for a future
    // completed directly (as submitCommand's NotLeaderException case is), but
    // defensively unwrap a CompletionException too in case that ever changes.
    private static Throwable unwrap(Throwable t) {
        return t instanceof CompletionException && t.getCause() != null ? t.getCause() : t;
    }
}
