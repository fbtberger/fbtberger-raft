package com.fbtberger.raft.transport;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.PreVoteRequest;
import com.fbtberger.raft.proto.PreVoteResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public final class TimeoutTransport implements RaftTransport {

    private final RaftTransport delegate;
    private final RpcTimeouts timeouts;

    public TimeoutTransport(RaftTransport delegate, RpcTimeouts timeouts) {
        this.delegate = delegate;
        this.timeouts = timeouts;
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return delegate.requestVote(request)
                .orTimeout(timeouts.requestVoteMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
        return delegate.appendEntries(request)
                .orTimeout(timeouts.appendEntriesMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        return delegate.installSnapshot(request)
                .orTimeout(timeouts.installSnapshotMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest request) {
        return delegate.preVote(request)
                .orTimeout(timeouts.preVoteMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest request) {
        return delegate.timeoutNow(request)
                .orTimeout(timeouts.requestVoteMs(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
