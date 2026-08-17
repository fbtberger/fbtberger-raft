/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

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
import com.fbtberger.raft.transport.RaftTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reproduces and proves the fix for a real bug found live on a 3-node cluster (Change 78/79
 * era): a node that logged its single "starting as FOLLOWER" line and then went completely
 * silent forever — no further raft logging at all, even at DEBUG, no election ever attempted
 * again. Root cause: {@code startElection()} iterated {@code peerTransports.values()} and
 * called {@code peer.preVote(...)} directly inside the {@code scheduler.schedule(...)} task
 * ({@link RaftNode#resetElectionTimer()}); if that call threw <em>synchronously</em> (e.g. a
 * peer transport lazily constructing its channel/TLS context and failing to do so) rather than
 * failing asynchronously via the returned future, the exception propagated out of the scheduled
 * {@code Runnable} entirely. {@code ScheduledExecutorService} swallows an uncaught exception
 * from a scheduled task completely silently — no log, no retry — and since
 * {@code resetElectionTimer()} (the only thing that schedules the *next* attempt) sat AFTER the
 * throwing call, no future election was ever scheduled either. One bad attempt, permanent silence.
 *
 * <p>This test injects a peer transport whose {@code preVote()} throws synchronously every
 * time, and asserts the node still attempts multiple elections over time — proving the fix
 * (wrapping the peer-transport calls, logging failures, and unconditionally calling
 * {@code resetElectionTimer()} afterward) actually keeps the node retrying instead of dying
 * after the first failure.
 */
class RaftNodeElectionResilienceTest {

    private RaftNode node;

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.shutdown();
        }
    }

    /** Always throws synchronously from preVote() — the exact failure mode this test targets. */
    private static final class SynchronouslyThrowingTransport implements RaftTransport {
        final AtomicInteger preVoteAttempts = new AtomicInteger();

        @Override
        public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest request) {
            preVoteAttempts.incrementAndGet();
            // The exact bug: a genuine synchronous throw, not a failed future — e.g. what a
            // lazily-constructed Netty channel/TLS context would do if setup itself fails,
            // rather than the RPC completing with an error.
            throw new RuntimeException("simulated synchronous transport failure (e.g. TLS setup)");
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
            return CompletableFuture.failedFuture(new RuntimeException("unused in this test"));
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
            return CompletableFuture.failedFuture(new RuntimeException("unused in this test"));
        }

        @Override
        public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
            return CompletableFuture.failedFuture(new RuntimeException("unused in this test"));
        }

        @Override
        public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest request) {
            return CompletableFuture.failedFuture(new RuntimeException("unused in this test"));
        }

        @Override
        public void close() {
        }
    }

    private static RaftConfig threeNodeConfig(String selfId) throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", selfId);
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-resilience-test-unused/" + selfId);
        props.setProperty("peer." + selfId, "localhost:9091");
        props.setProperty("peer.other1", "localhost:9092");
        props.setProperty("peer.other2", "localhost:9093");
        props.setProperty("snapshot.threshold", "20");

        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-resilience-test-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    @Test
    @DisplayName("a peer whose preVote() throws synchronously does not permanently silence "
            + "future election attempts — the exact bug found live on a real 3-node cluster")
    void survivesSynchronousPreVoteFailureAndKeepsRetrying() throws Exception {
        SynchronouslyThrowingTransport throwingPeer = new SynchronouslyThrowingTransport();
        RaftConfig config = threeNodeConfig("self");

        node = new RaftNode(config, new InMemoryStorage(), new KeyValueStateMachine(),
                address -> throwingPeer, RaftMetrics.noop());
        node.start();

        // Election timeout is 150-300ms; two elections need at least ~300ms, so wait well
        // beyond that. If the bug were still present, exactly one attempt would ever happen,
        // no matter how long we wait.
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline && throwingPeer.preVoteAttempts.get() < 3) {
            Thread.sleep(50);
        }

        assertTrue(throwingPeer.preVoteAttempts.get() >= 3,
                "expected multiple retried election attempts despite the peer always throwing "
                        + "synchronously; got only " + throwingPeer.preVoteAttempts.get()
                        + " — the node went silent after too few attempts, reproducing the bug");
    }
}
