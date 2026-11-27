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
import com.fbtberger.raft.proto.ReadIndexRequest;
import com.fbtberger.raft.proto.ReadIndexResponse;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import com.fbtberger.raft.transport.RaftTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The ways a follower read must fail. A read that succeeds is shown on a real cluster in
 * {@code MultiTransportClusterTest}; what cannot be shown there, because a healthy cluster
 * never does it, is that a follower which cannot prove it is current says so instead of
 * answering. The leader here is a script.
 */
class FollowerReadTest {

    private static final Map<String, String> PEERS = Map.of(
            "f1", "localhost:9301", "lead", "localhost:9302", "f3", "localhost:9303");
    private static final byte[] GET = "GET k".getBytes(StandardCharsets.UTF_8);

    /** What the scripted leader answers to ReadIndex; everything else it leaves unanswered. */
    private final AtomicReference<Function<ReadIndexRequest, CompletableFuture<ReadIndexResponse>>> leaderAnswers =
            new AtomicReference<>();
    private RaftNode node;

    @AfterEach
    void stop() {
        if (node != null) node.shutdown();
    }

    /** A node that has heard from "lead" once and therefore follows it. Never started, so it never campaigns. */
    private RaftNode followerOf(String leaderId) {
        RaftConfig config = RaftConfig.of("f1", 9301, Path.of("unused"), PEERS);
        node = new RaftNode(config, new InMemoryStorage(), new KeyValueStateMachine(),
                address -> new ScriptedLeader(), RaftMetrics.noop());
        if (leaderId != null) {
            AppendEntriesResponse heard = node.handleAppendEntries(AppendEntriesRequest.newBuilder()
                    .setTerm(1).setLeaderId(leaderId).build());
            assertTrue(heard.getSuccess());
            assertEquals(leaderId, node.currentLeaderId());
        }
        return node;
    }

    private static Throwable causeOf(CompletableFuture<?> future) {
        ExecutionException e = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
        return e.getCause();
    }

    @Test
    void aNodeThatKnowsNoLeaderRefusesInsteadOfReadingItsOwnState() {
        RaftNode orphan = followerOf(null);

        Throwable cause = causeOf(orphan.queryAnyRole(GET));

        assertInstanceOf(RaftNode.NotLeaderException.class, cause);
    }

    @Test
    void aFollowerWhoseLeaderIsUnreachableFailsTheRead() {
        RaftNode follower = followerOf("lead");
        leaderAnswers.set(request -> CompletableFuture.failedFuture(new IOException("connection refused")));

        Throwable cause = causeOf(follower.queryAnyRole(GET));

        // This is the partitioned follower: it still believes in its leader and has state
        // to read from. Without an index from the leader it has no right to.
        assertInstanceOf(IOException.class, cause);
    }

    @Test
    void aRefusalFromADeposedLeaderIsPassedOnWithItsHint() {
        RaftNode follower = followerOf("lead");
        leaderAnswers.set(request -> CompletableFuture.completedFuture(
                ReadIndexResponse.newBuilder().setSuccess(false).setLeaderHint("f3").build()));

        Throwable cause = causeOf(follower.queryAnyRole(GET));

        assertInstanceOf(RaftNode.NotLeaderException.class, cause);
        assertEquals("f3", ((RaftNode.NotLeaderException) cause).leaderHint);
    }

    @Test
    void aFollowerTooFarBehindGivesUpInsteadOfAnsweringEarly() {
        RaftNode follower = followerOf("lead");
        // The leader says the read must see index 50. This follower has applied nothing.
        leaderAnswers.set(request -> CompletableFuture.completedFuture(
                ReadIndexResponse.newBuilder().setSuccess(true).setReadIndex(50).build()));

        CompletableFuture<byte[]> read = follower.queryAnyRole(GET);

        assertThrows(java.util.concurrent.TimeoutException.class, () -> read.get(300, TimeUnit.MILLISECONDS),
                "the read was answered before the follower had applied up to the read index");
        Throwable cause = causeOf(read);
        assertInstanceOf(RaftNode.FollowerReadException.class, cause);
        assertTrue(cause.getMessage().contains("50"), cause.getMessage());
    }

    @Test
    void theFollowerNamesItselfSoTheLeaderCanDecideWhetherToAnswer() {
        RaftNode follower = followerOf("lead");
        AtomicReference<String> requester = new AtomicReference<>();
        leaderAnswers.set(request -> {
            requester.set(request.getRequesterId());
            return CompletableFuture.completedFuture(ReadIndexResponse.getDefaultInstance());
        });

        causeOf(follower.readIndexAnyRole());

        assertEquals("f1", requester.get());
    }

    @Test
    void aNodeThatIsNotLeaderAnswersAReadIndexRequestWithAHintNotAnIndex() throws Exception {
        RaftNode follower = followerOf("lead");

        ReadIndexResponse response = follower.handleReadIndex(
                ReadIndexRequest.newBuilder().setRequesterId("f3").build()).get(1, TimeUnit.SECONDS);

        assertFalse(response.getSuccess());
        assertEquals("lead", response.getLeaderHint());
    }

    @Test
    void aLeaderRefusesAReadIndexToSomeoneOutsideItsConfiguration() throws Exception {
        RaftConfig config = RaftConfig.of("solo", 9401, Path.of("unused"), Map.of("solo", "localhost:9401"));
        node = new RaftNode(config, new InMemoryStorage(), new KeyValueStateMachine(),
                address -> null, RaftMetrics.noop());
        node.start();
        Await.until("the single node to lead", 5_000, () -> node.role() == ServerRole.LEADER);

        ReadIndexResponse stranger = node.handleReadIndex(
                ReadIndexRequest.newBuilder().setRequesterId("nobody").build()).get(1, TimeUnit.SECONDS);
        ReadIndexResponse member = node.handleReadIndex(
                ReadIndexRequest.newBuilder().setRequesterId("solo").build()).get(1, TimeUnit.SECONDS);

        assertFalse(stranger.getSuccess());
        assertTrue(member.getSuccess());
    }

    @Test
    void aNewLeadersReadIndexIsNeverBelowItsOwnFirstEntry() throws Exception {
        // §6.4 step 1. A fresh leader's commit index is what it knew as a follower -- here 0
        // -- until its no-op commits. A read index taken from it alone would let a read run
        // before entries its predecessor had already committed.
        RaftConfig config = RaftConfig.of("solo", 9401, Path.of("unused"), Map.of("solo", "localhost:9401"));
        node = new RaftNode(config, new InMemoryStorage(), new KeyValueStateMachine(),
                address -> null, RaftMetrics.noop());
        node.start();
        Await.until("the single node to lead", 5_000, () -> node.role() == ServerRole.LEADER);

        assertTrue(node.readIndex().get(2, TimeUnit.SECONDS) >= 1,
                "the no-op at index 1 is the floor for every read in this term");
    }

    private final class ScriptedLeader implements RaftTransport {
        @Override public CompletableFuture<ReadIndexResponse> readIndex(ReadIndexRequest request) {
            return leaderAnswers.get().apply(request);
        }
        @Override public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest r) {
            return new CompletableFuture<>();
        }
        @Override public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest r) {
            return new CompletableFuture<>();
        }
        @Override public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest r) {
            return new CompletableFuture<>();
        }
        @Override public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest r) {
            return new CompletableFuture<>();
        }
        @Override public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest r) {
            return new CompletableFuture<>();
        }
        @Override public void close() { }
    }
}
