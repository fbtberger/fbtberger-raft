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
import com.fbtberger.raft.client.proto.QueryRequest;
import com.fbtberger.raft.client.proto.QueryResponse;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import com.fbtberger.raft.client.proto.SubmitRequest;
import com.fbtberger.raft.client.proto.SubmitResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import com.fbtberger.raft.transport.RaftTransport;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A leader whose followers have stopped acknowledging: commands are accepted and cannot
 * commit. Without a limit that queue grows for as long as clients keep sending.
 */
class OverloadProtectionTest {

    private final AtomicBoolean followersAcknowledge = new AtomicBoolean(true);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private InMemoryStorage store;
    private RaftNode leader;

    @AfterEach
    void tearDown() {
        if (leader != null) leader.shutdown();
    }

    @Test
    void commandsBeyondTheLimitAreRefusedAndNotAppended() throws Exception {
        startLeader(/*maxWrites*/ 3, /*maxReads*/ 100);
        followersAcknowledge.set(false);

        List<CompletableFuture<byte[]>> accepted = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            accepted.add(leader.submitCommand(("SET k" + i + " " + i).getBytes()));
        }
        long lastIndexBefore = store.getLastLogIndex();
        CompletableFuture<byte[]> fourth = leader.submitCommand("SET k3 3".getBytes());
        CompletableFuture<byte[]> tagged = leader.submitCommand("SET k4 4".getBytes(), "client-1", 1, 0);

        for (CompletableFuture<byte[]> refused : List.of(fourth, tagged)) {
            ExecutionException failure =
                    assertThrows(ExecutionException.class, () -> refused.get(1, TimeUnit.SECONDS));
            RaftNode.OverloadedException overloaded =
                    assertInstanceOf(RaftNode.OverloadedException.class, failure.getCause());
            assertTrue(overloaded.retryAfterMs > 0);
        }
        assertEquals(lastIndexBefore, store.getLastLogIndex(), "a refused command must leave no trace in the log");
        assertEquals(0, leader.clientSessionCount());
        assertTrue(accepted.stream().noneMatch(CompletableFuture::isDone), "what was accepted is still waiting");
        assertEquals(2.0, registry.get("raft.client.overloaded").tag("kind", "write").counter().count());
        assertEquals(3.0, registry.get("raft.client.pending").tag("kind", "write").gauge().value());
    }

    @Test
    void whatWasAcceptedCommitsAndThenThereIsRoomAgain() throws Exception {
        startLeader(3, 100);
        followersAcknowledge.set(false);
        List<CompletableFuture<byte[]>> accepted = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            accepted.add(leader.submitCommand(("SET k" + i + " " + i).getBytes()));
        }
        assertTrue(leader.submitCommand("SET late 1".getBytes()).isCompletedExceptionally());

        followersAcknowledge.set(true);

        for (CompletableFuture<byte[]> future : accepted) {
            future.get(3, TimeUnit.SECONDS);
        }
        leader.submitCommand("SET late 1".getBytes()).get(3, TimeUnit.SECONDS);
        assertEquals(0.0, registry.get("raft.client.pending").tag("kind", "write").gauge().value());
    }

    @Test
    void readsBeyondTheLimitAreRefused() throws Exception {
        startLeader(100, /*maxReads*/ 2);
        followersAcknowledge.set(false);

        CompletableFuture<Long> first = leader.readIndex();
        CompletableFuture<Long> second = leader.readIndex();
        CompletableFuture<byte[]> third = leader.query("GET a".getBytes());

        ExecutionException failure = assertThrows(ExecutionException.class, () -> third.get(1, TimeUnit.SECONDS));
        assertInstanceOf(RaftNode.OverloadedException.class, failure.getCause());
        assertFalse(first.isDone());
        assertFalse(second.isDone());
        assertEquals(1.0, registry.get("raft.client.overloaded").tag("kind", "read").counter().count());

        followersAcknowledge.set(true);
        first.get(3, TimeUnit.SECONDS);
        second.get(3, TimeUnit.SECONDS);
        leader.readIndex().get(3, TimeUnit.SECONDS);
    }

    @Test
    void zeroSwitchesTheLimitsOff() throws Exception {
        startLeader(0, 0);
        followersAcknowledge.set(false);

        for (int i = 0; i < 200; i++) {
            assertFalse(leader.submitCommand(("SET k" + i + " " + i).getBytes()).isCompletedExceptionally());
            assertFalse(leader.readIndex().isCompletedExceptionally());
        }
    }

    @Test
    void theClientServiceSaysOverloadedAndWhenToComeBack() throws Exception {
        startLeader(1, 1);
        followersAcknowledge.set(false);
        leader.submitCommand("SET a 1".getBytes());
        leader.readIndex();
        RaftClientGrpcService service = new RaftClientGrpcService(leader);

        CompletableFuture<SubmitResponse> submit = new CompletableFuture<>();
        service.submit(SubmitRequest.newBuilder().setCommand(ByteString.copyFromUtf8("SET b 2")).build(),
                observer(submit));
        CompletableFuture<QueryResponse> query = new CompletableFuture<>();
        service.query(QueryRequest.newBuilder().setQuery(ByteString.copyFromUtf8("GET a")).build(),
                observer(query));

        SubmitResponse refusedWrite = submit.get(1, TimeUnit.SECONDS);
        assertFalse(refusedWrite.getSuccess());
        assertTrue(refusedWrite.getOverloaded());
        assertEquals(RaftNode.OverloadedException.RETRY_AFTER_MS, refusedWrite.getRetryAfterMs());
        assertTrue(refusedWrite.getLeaderHint().isEmpty(), "there is nowhere else to go: this IS the leader");
        QueryResponse refusedRead = query.get(1, TimeUnit.SECONDS);
        assertTrue(refusedRead.getOverloaded());
        assertEquals(RaftNode.OverloadedException.RETRY_AFTER_MS, refusedRead.getRetryAfterMs());
    }

    @Test
    void theLimitsComeFromThePropertiesFileAndHaveDefaults() throws Exception {
        assertEquals(7, config(7, 9).maxPendingWrites());
        assertEquals(9, config(7, 9).maxPendingReads());

        Properties props = properties();
        RaftConfig defaults = load(props);
        assertEquals(RaftConfig.DEFAULT_MAX_PENDING, defaults.maxPendingWrites());
        assertEquals(RaftConfig.DEFAULT_MAX_PENDING, defaults.maxPendingReads());
        assertEquals(7, config(7, 9).asWitness(5).maxPendingWrites(), "a copy keeps the limits");

        props.setProperty("overload.max.pending.writes", "-1");
        assertThrows(IllegalArgumentException.class, () -> load(props));
    }

    // ---- helpers -----------------------------------------------------------

    private static <T> StreamObserver<T> observer(CompletableFuture<T> into) {
        return new StreamObserver<>() {
            @Override public void onNext(T value) { into.complete(value); }
            @Override public void onError(Throwable t) { into.completeExceptionally(t); }
            @Override public void onCompleted() { }
        };
    }

    private void startLeader(int maxWrites, int maxReads) throws Exception {
        store = new InMemoryStorage();
        leader = new RaftNode(config(maxWrites, maxReads), store, new KeyValueStateMachine(),
                address -> new SwitchableFollower(), new RaftMetrics(registry, "n1"));
        leader.start();
        Await.until("n1 leads and has committed in its term", 5_000, () -> {
            if (leader.role() != ServerRole.LEADER) return false;
            try {
                leader.submitCommand("SET warm up".getBytes()).get(200, TimeUnit.MILLISECONDS);
                return true;
            } catch (Exception e) {
                return false;
            }
        });
    }

    private static RaftConfig config(int maxWrites, int maxReads) throws IOException {
        Properties props = properties();
        props.setProperty("overload.max.pending.writes", Integer.toString(maxWrites));
        props.setProperty("overload.max.pending.reads", Integer.toString(maxReads));
        return load(props);
    }

    private static Properties properties() {
        Properties props = new Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9391");
        props.setProperty("data.dir", "/tmp/raft-overload-unused/n1");
        props.setProperty("peer.n1", "localhost:9391");
        props.setProperty("peer.n2", "localhost:9392");
        props.setProperty("peer.n3", "localhost:9393");
        return props;
    }

    private static RaftConfig load(Properties props) throws IOException {
        Path file = Files.createTempFile("raft-overload-", ".properties");
        try (OutputStream out = Files.newOutputStream(file)) {
            props.store(out, null);
        }
        return RaftConfig.load(file);
    }

    /** Votes for anyone; acknowledges appends only while the switch is on. */
    private final class SwitchableFollower implements RaftTransport {
        @Override
        public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest r) {
            if (!followersAcknowledge.get()) {
                return CompletableFuture.failedFuture(new IOException("follower does not answer"));
            }
            return CompletableFuture.completedFuture(AppendEntriesResponse.newBuilder()
                    .setTerm(r.getTerm()).setSuccess(true).build());
        }

        @Override
        public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest r) {
            return CompletableFuture.completedFuture(RequestVoteResponse.newBuilder()
                    .setTerm(r.getTerm()).setVoteGranted(true).build());
        }

        @Override
        public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest r) {
            return CompletableFuture.completedFuture(PreVoteResponse.newBuilder()
                    .setTerm(0).setVoteGranted(true).build());
        }

        @Override
        public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest r) {
            return CompletableFuture.completedFuture(
                    InstallSnapshotResponse.newBuilder().setTerm(r.getTerm()).build());
        }

        @Override
        public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest r) {
            return CompletableFuture.completedFuture(TimeoutNowResponse.getDefaultInstance());
        }

        @Override
        public void close() { }
    }
}
