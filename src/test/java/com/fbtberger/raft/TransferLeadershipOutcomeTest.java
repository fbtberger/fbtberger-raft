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
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a leadership transfer reports when its own success destroys the answer.
 *
 * <p>The target campaigns the instant it receives {@code TimeoutNow}, wins, and the old
 * leader steps down -- which tears down the call whose response it is still waiting for.
 * gRPC reports CANCELLED. Taking that at face value means the operation reports failure
 * exactly when it worked, and reports it faster the better it worked.
 *
 * <p>Not hypothetical. In a rehearsal against the Pi cluster the operator command printed
 * "the cluster refused: node3 rejected the request (CANCELLED: io.grpc.Context was cancelled
 * without error)" while the same run's leader log recorded "node3 -&gt; node1 (gap 0.00 s)".
 * On a stage that is a red line under a step that did exactly what was announced.
 *
 * <p>Both directions are here, because the fix is a judgement about which failures are real:
 * a transfer that ends with this node no longer leading succeeded; one that ends with it
 * still leading did not, and must keep saying so.
 */
class TransferLeadershipOutcomeTest {

    private RaftNode node;
    private InMemoryStorage store;

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.shutdown();
        }
    }

    @Test
    void aCancelledResponseAfterSteppingDownIsNotAFailedTransfer() throws Exception {
        ControllableTransport peer = new ControllableTransport();
        node = leaderWith(peer);

        CompletableFuture<Void> transfer = node.transferLeadership("n2");
        assertTrue(peer.timeoutNowSent.await(3, TimeUnit.SECONDS),
                "the target was never told to campaign");

        // What winning looks like from here: the target's new term arrives and this node
        // stops being the leader. Delivered through the public RPC surface, so the test
        // depends on the same path the network does.
        node.handleAppendEntries(AppendEntriesRequest.newBuilder()
                .setTerm(store.getCurrentTerm() + 1).setLeaderId("n2")
                .setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(0).build());
        assertFalse(node.role() == ServerRole.LEADER, "the node should have stepped down");

        // Only now does the response fail -- the order a real cancellation arrives in.
        peer.timeoutNowResult.completeExceptionally(
                new RuntimeException("CANCELLED: io.grpc.Context was cancelled without error"));

        transfer.get(3, TimeUnit.SECONDS);   // must not throw
    }

    @Test
    void aFailedTimeoutNowWhileStillLeadingIsStillAFailure() throws Exception {
        ControllableTransport peer = new ControllableTransport();
        node = leaderWith(peer);

        CompletableFuture<Void> transfer = node.transferLeadership("n2");
        assertTrue(peer.timeoutNowSent.await(3, TimeUnit.SECONDS));

        // Nothing changed: the message did not get through and this node still leads. That
        // is a real failure and swallowing it would make the command useless -- it would
        // succeed whatever happened.
        peer.timeoutNowResult.completeExceptionally(new RuntimeException("UNAVAILABLE"));

        ExecutionException thrown = assertThrows(ExecutionException.class,
                () -> transfer.get(3, TimeUnit.SECONDS));
        assertTrue(thrown.getCause().getMessage().contains("UNAVAILABLE"),
                thrown.getCause().getMessage());
    }

    /** A two-member cluster whose peer answers AppendEntries and holds TimeoutNow open. */
    private RaftNode leaderWith(ControllableTransport peer) throws Exception {
        store = new InMemoryStorage();
        RaftNode n = new RaftNode(config(), store, new KeyValueStateMachine(),
                addr -> peer, RaftMetrics.noop());
        n.start();
        long deadline = System.currentTimeMillis() + 3_000;
        while (n.role() != ServerRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        n.addServer("n2", "localhost:9092").get(3, TimeUnit.SECONDS);
        n.submitCommand("SET a 1".getBytes(StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);
        return n;
    }

    private static RaftConfig config() throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-transfer-outcome-unused");
        props.setProperty("peer.n1", "localhost:9091");
        props.setProperty("snapshot.threshold", "1000000");
        Path tmp = Files.createTempFile("raft-transfer-outcome-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    /**
     * Agrees with everything, except that its {@code TimeoutNow} answer is whatever the test
     * decides and whenever the test decides it -- which is the whole point: the interesting
     * cases are about when the answer arrives relative to the step-down.
     */
    private static final class ControllableTransport implements RaftTransport {

        final CountDownLatch timeoutNowSent = new CountDownLatch(1);
        final CompletableFuture<TimeoutNowResponse> timeoutNowResult = new CompletableFuture<>();
        final AtomicReference<TimeoutNowRequest> lastRequest = new AtomicReference<>();

        @Override
        public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest request) {
            lastRequest.set(request);
            timeoutNowSent.countDown();
            return timeoutNowResult;
        }

        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
            return CompletableFuture.completedFuture(AppendEntriesResponse.newBuilder()
                    .setTerm(request.getTerm())
                    .setSuccess(true)
                    .build());
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
            return CompletableFuture.completedFuture(RequestVoteResponse.newBuilder()
                    .setTerm(request.getTerm()).setVoteGranted(true).build());
        }

        @Override
        public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest request) {
            return CompletableFuture.completedFuture(PreVoteResponse.newBuilder()
                    .setTerm(request.getTerm()).setVoteGranted(true).build());
        }

        @Override
        public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
            return CompletableFuture.completedFuture(
                    InstallSnapshotResponse.newBuilder().setTerm(request.getTerm()).build());
        }

        @Override
        public void close() {
        }
    }
}
