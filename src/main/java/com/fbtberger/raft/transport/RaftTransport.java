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
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
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
     * Hint that the peer behind this transport may have returned, so any reconnect backoff should
     * be abandoned and the next attempt made immediately. Cheap, idempotent, and safe to call on
     * every failure -- unlike closing and rebuilding the transport, which is the only other lever
     * a caller has and is expensive enough to need throttling.
     *
     * <p>Default is a no-op: a transport with no connection state (in-process, test doubles) has
     * nothing to reset, and implementations should only override this if backing off is something
     * they actually do.
     *
     * <p>Best-effort by contract. It asks for an earlier attempt; it does not promise the attempt
     * succeeds, nor that any name resolution behind it is refreshed.
     */
    default void resetBackoff() { }

    @Override void close();
}
