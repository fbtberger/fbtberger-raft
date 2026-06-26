package com.fbtberger.raft.transport;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;

public interface RaftTransport extends AutoCloseable {
    CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);
    CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request);
    CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request);
    @Override void close();
}
