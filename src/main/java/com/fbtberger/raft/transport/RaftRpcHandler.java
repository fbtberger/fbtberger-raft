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
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import java.util.concurrent.CompletableFuture;

public interface RaftRpcHandler {
    RequestVoteResponse handleRequestVote(RequestVoteRequest request);
    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request);
    InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request);
    PreVoteResponse handlePreVote(PreVoteRequest request);
    TimeoutNowResponse handleTimeoutNow(TimeoutNowRequest request);

    /**
     * Answers a follower's request for a read index (§6.4). Asynchronous, unlike its
     * neighbours: the answer needs a heartbeat round, and a handler that blocked for
     * one would hold a transport thread for every read in flight.
     *
     * <p>The default refuses, so a handler that predates follower reads says "not
     * here" instead of failing to compile or answering something it cannot back.
     */
    default CompletableFuture<ReadIndexResponse> handleReadIndex(ReadIndexRequest request) {
        return CompletableFuture.completedFuture(ReadIndexResponse.getDefaultInstance());
    }
}
