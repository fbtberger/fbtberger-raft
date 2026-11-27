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
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import com.fbtberger.raft.proto.ReadIndexRequest;
import com.fbtberger.raft.proto.ReadIndexResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;

import java.util.concurrent.CompletableFuture;

public interface RaftTransport extends AutoCloseable {
    CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request);
    CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request);
    CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request);
    CompletableFuture<PreVoteResponse> preVote(PreVoteRequest request);
    CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest request);

    /**
     * Asks the peer, as leader, for a linearizable read index (§6.4 follower reads).
     *
     * <p>A default rather than an abstract method: a transport that predates follower
     * reads keeps compiling and keeps working for everything else, and a follower read
     * over it fails with a reason instead of hanging.
     */
    default CompletableFuture<ReadIndexResponse> readIndex(ReadIndexRequest request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException(
                getClass().getSimpleName() + " does not carry the ReadIndex RPC"));
    }
    @Override void close();
}
