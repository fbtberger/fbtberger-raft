package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.RaftServiceGrpc;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Timer;

/**
 * Adapter between the generated gRPC service stubs and {@link RaftNode}.
 * <p>
 * This class contains no Raft logic of its own (per Figure 2 of the paper) — it only
 * unwraps incoming requests, hands them to {@link RaftNode#handleRequestVote} /
 * {@link RaftNode#handleAppendEntries} / {@link RaftNode#handleInstallSnapshot}, and
 * reports the result (or any failure) back to gRPC via the {@link StreamObserver}.
 * All three RaftNode handler methods are written to run synchronously and return
 * promptly, so no extra threading is introduced here; gRPC's own executor handles
 * concurrency across incoming RPCs.
 */
public final class RaftGrpcService extends RaftServiceGrpc.RaftServiceImplBase {

    private final RaftNode raftNode;
    private final RaftMetrics metrics;

    public RaftGrpcService(RaftNode raftNode) {
        this.raftNode = raftNode;
        this.metrics = raftNode.metrics();
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
        Timer.Sample sample = Timer.start(metrics.registry());
        try {
            RequestVoteResponse response = raftNode.handleRequestVote(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        } finally {
            sample.stop(metrics.requestVoteTimer());
        }
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
        Timer.Sample sample = Timer.start(metrics.registry());
        try {
            AppendEntriesResponse response = raftNode.handleAppendEntries(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        } finally {
            sample.stop(metrics.appendEntriesTimer());
        }
    }

    @Override
    public void installSnapshot(InstallSnapshotRequest request, StreamObserver<InstallSnapshotResponse> responseObserver) {
        Timer.Sample sample = Timer.start(metrics.registry());
        try {
            InstallSnapshotResponse response = raftNode.handleInstallSnapshot(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            responseObserver.onError(e);
        } finally {
            sample.stop(metrics.installSnapshotTimer());
        }
    }
}
