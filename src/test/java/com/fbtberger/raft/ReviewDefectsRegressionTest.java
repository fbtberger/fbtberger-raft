/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.ClusterConfiguration;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.LogEntry;
import com.fbtberger.raft.proto.PreVoteRequest;
import com.fbtberger.raft.proto.PreVoteResponse;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import com.fbtberger.raft.transport.RaftTransport;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The five defects the review of 2026-11-27 found in {@link RaftNode}, one test each, plus
 * the consequence of fixing the third. Every one of them failed before its fix.
 *
 * <p>They share a file because they share a harness: a node driven through its RPC surface,
 * with transports that answer whatever the case needs.
 */
class ReviewDefectsRegressionTest {

    private final List<RaftNode> nodes = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (RaftNode n : nodes) n.shutdown();
    }

    private static LogEntry set(long index, long term, String k) {
        return LogEntry.newBuilder().setIndex(index).setTerm(term)
                .setCommand(ByteString.copyFromUtf8("SET " + k + " v")).build();
    }

    private static String get(KeyValueStateMachine sm, String k) {
        return new String(sm.read(("GET " + k).getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    /**
     * A leader sends a snapshot whenever its nextIndex for a follower falls to its snapshot
     * boundary -- one failed AppendEntries right after an election does that -- so a follower
     * can be offered a snapshot that ends below what it has applied. It used to install it:
     * the state went back to the snapshot, lastApplied stayed, the entries between were lost.
     */
    @Test
    void installSnapshotBehindAppliedIndexMustNotRollBackState() throws Exception {
        KeyValueStateMachine sm = new KeyValueStateMachine();
        RaftNode f = new RaftNode(config("f", "f", "a", "b"), new InMemoryStorage(), sm, addr -> null, RaftMetrics.noop());
        nodes.add(f);
        AppendEntriesRequest.Builder seed = AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("a").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(10);
        for (int i = 1; i <= 10; i++) seed.addEntries(set(i, 1, "k" + i));
        assertTrue(f.handleAppendEntries(seed.build()).getSuccess());
        assertEquals(10, f.appliedIndex());
        String before = get(sm, "k10");

        KeyValueStateMachine at5 = new KeyValueStateMachine();
        for (int i = 1; i <= 5; i++) at5.apply(("SET k" + i + " v").getBytes(StandardCharsets.UTF_8));
        byte[] packed = RaftNode.packSnapshotData(at5.takeSnapshot(), new byte[0]);
        f.handleInstallSnapshot(InstallSnapshotRequest.newBuilder()
                .setTerm(1).setLeaderId("a").setLastIncludedIndex(5).setLastIncludedTerm(1)
                .setOffset(0).setData(ByteString.copyFrom(packed)).setDone(true).build());

        assertEquals(10, f.appliedIndex());
        assertEquals(before, get(sm, "k10"), "state for an applied entry vanished after InstallSnapshot");
    }

    /** A batch that restarts from the beginning of the log ends below our commitIndex. */
    @Test
    void commitIndexMustNotMoveBackwards() throws Exception {
        RaftNode f = new RaftNode(config("f", "f", "a", "b"), new InMemoryStorage(),
                new KeyValueStateMachine(), addr -> null, RaftMetrics.noop());
        nodes.add(f);
        AppendEntriesRequest.Builder seed = AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("a").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(10);
        for (int i = 1; i <= 10; i++) seed.addEntries(set(i, 1, "k" + i));
        f.handleAppendEntries(seed.build());
        assertEquals(10, f.commitIndex());

        AppendEntriesRequest.Builder resend = AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("a").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(20);
        for (int i = 1; i <= 3; i++) resend.addEntries(set(i, 1, "k" + i));
        f.handleAppendEntries(resend.build());
        assertEquals(10, f.commitIndex(), "commitIndex went backwards");
    }

    /**
     * Requests wait by log index. After a step-down the next leader may put its own entry at
     * that index, and the waiting client used to get that entry's result as its own.
     */
    @Test
    void pendingClientRequestMustNotBeAnsweredWithAnotherLeadersEntry() throws Exception {
        InMemoryStorage store = new InMemoryStorage();
        Fake votesOnly = new Fake(true, null);   // grants votes, never answers AppendEntries
        RaftNode n = new RaftNode(config("n1", "n1", "a", "b"), store, new KeyValueStateMachine(),
                addr -> votesOnly, RaftMetrics.noop());
        nodes.add(n);
        n.start();
        awaitLeader(n);
        long term = store.getCurrentTerm();
        CompletableFuture<byte[]> lost = n.submitCommand("SET mine 1".getBytes(StandardCharsets.UTF_8));
        long lostIndex = store.getLastLogIndex();

        AppendEntriesRequest.Builder other = AppendEntriesRequest.newBuilder()
                .setTerm(term + 1).setLeaderId("a").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(lostIndex);
        for (long i = 1; i <= lostIndex; i++) other.addEntries(set(i, term + 1, "theirs" + i));
        assertTrue(n.handleAppendEntries(other.build()).getSuccess());

        boolean completedNormally;
        try {
            lost.get(1, TimeUnit.SECONDS);
            completedNormally = true;
        } catch (Exception e) {
            completedNormally = false;
        }
        assertTrue(!completedNormally, "client was told its write succeeded, but the entry was overwritten");
    }

    /** Votes were counted over the union of C_old and C_new against the larger majority. */
    @Test
    void candidateInJointConfigurationNeedsAMajorityOfTheOldOneToo() throws Exception {
        InMemoryStorage store = new InMemoryStorage();
        ClusterConfiguration.Builder joint = ClusterConfiguration.newBuilder();
        for (String id : List.of("d", "e", "f")) joint.addMembers(member(id));
        for (String id : List.of("a", "b", "c")) joint.addOldMembers(member(id));
        store.setTermAndVote(1, null);
        store.appendEntries(List.of(LogEntry.newBuilder().setIndex(1).setTerm(1).setConfiguration(joint).build()));

        Fake grants = new Fake(true, true);
        Fake silent = new Fake(null, null);
        RaftNode d = new RaftNode(config("d", "d", "e", "f"), store, new KeyValueStateMachine(),
                addr -> addr.contains("-new-") ? grants : silent, RaftMetrics.noop());
        nodes.add(d);
        d.start();
        Thread.sleep(3_000);
        assertNotEquals(ServerRole.LEADER, d.role(),
                "elected with votes from C_new only; no member of C_old voted");
    }

    /**
     * A leader that finds a committed C_old,new in its log finishes the change. Before the
     * election fix a step-down simply forgot C_old, which hid that nobody did.
     */
    @Test
    void aLeaderThatInheritsAJointConfigurationProposesTheFinalOne() throws Exception {
        InMemoryStorage store = new InMemoryStorage();
        ClusterConfiguration.Builder joint = ClusterConfiguration.newBuilder();
        for (String id : List.of("d", "e", "f")) joint.addMembers(member(id));
        for (String id : List.of("a", "b", "c")) joint.addOldMembers(member(id));
        store.setTermAndVote(1, null);
        store.appendEntries(List.of(LogEntry.newBuilder().setIndex(1).setTerm(1).setConfiguration(joint).build()));

        Fake agrees = new Fake(true, true);
        RaftNode d = new RaftNode(config("d", "d", "e", "f"), store, new KeyValueStateMachine(),
                addr -> agrees, RaftMetrics.noop());
        nodes.add(d);
        d.start();
        awaitLeader(d);

        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline && store.getLastLogIndex() < 3) Thread.sleep(20);
        LogEntry last = store.getLogEntry(store.getLastLogIndex());
        assertTrue(last.hasConfiguration(), "no final configuration was proposed");
        assertEquals(0, last.getConfiguration().getOldMembersCount());
        assertEquals(3, last.getConfiguration().getMembersCount());
    }

    private static ClusterConfiguration.Member member(String id) {
        boolean isNew = "def".contains(id);
        return ClusterConfiguration.Member.newBuilder().setId(id)
                .setAddress("host-" + (isNew ? "new" : "old") + "-" + id + ":1").build();
    }

    /**
     * After a refusal nextIndex was capped at the last index sent rather than at the index
     * refused; with entries in the request that is no step back at all.
     */
    @Test
    void leaderBringsALongerDivergentFollowerBackInLine() throws Exception {
        // Follower: 1..2 term 1 (shared), 3..8 term 1 (never committed, divergent).
        InMemoryStorage fStore = new InMemoryStorage();
        RaftNode f = new RaftNode(config("f", "l", "f", "g"), fStore, new KeyValueStateMachine(),
                addr -> null, RaftMetrics.noop());
        nodes.add(f);
        AppendEntriesRequest.Builder seed = AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("old").setPrevLogIndex(0).setPrevLogTerm(0).setLeaderCommit(2);
        for (int i = 1; i <= 8; i++) seed.addEntries(set(i, 1, "k" + i));
        assertTrue(f.handleAppendEntries(seed.build()).getSuccess());

        // Leader: 1..2 term 1 (shared), 3 term 2.
        InMemoryStorage lStore = new InMemoryStorage();
        List<LogEntry> seedL = new ArrayList<>();
        seedL.add(set(1, 1, "k1"));
        seedL.add(set(2, 1, "k2"));
        seedL.add(set(3, 2, "other3"));
        lStore.appendEntries(seedL);
        lStore.setTermAndVote(2, null);

        Fake grants = new Fake(true, true);
        RaftTransport toFollower = new Delegating(f);
        RaftNode l = new RaftNode(config("l", "l", "f", "g"), lStore, new KeyValueStateMachine(),
                addr -> addr.contains("-f:") ? toFollower : grants, RaftMetrics.noop());
        nodes.add(l);
        l.start();
        awaitLeader(l);

        long deadline = System.currentTimeMillis() + 5_000;
        while (System.currentTimeMillis() < deadline
                && !(fStore.getLastLogIndex() == lStore.getLastLogIndex() && fStore.getTermAt(3) == 2)) {
            Thread.sleep(20);
        }
        assertEquals(2, fStore.getTermAt(3), "follower still holds its divergent entry at index 3");
        assertEquals(lStore.getLastLogIndex(), fStore.getLastLogIndex());
    }

    // -----------------------------------------------------------------

    private static void awaitLeader(RaftNode n) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000;
        while (n.role() != ServerRole.LEADER && System.currentTimeMillis() < deadline) Thread.sleep(10);
        assertEquals(ServerRole.LEADER, n.role());
    }

    private static RaftConfig config(String self, String... peers) throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", self);
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-review-unused/" + self);
        for (String p : peers) props.setProperty("peer." + p, "host-new-" + p + ":1");
        props.setProperty("snapshot.threshold", "1000000");
        Path tmp = Files.createTempFile("raft-review-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    /** votes: null = never answers; append: null = never answers. */
    private static final class Fake implements RaftTransport {
        final Boolean votes;
        final Boolean append;

        Fake(Boolean votes, Boolean append) {
            this.votes = votes;
            this.append = append;
        }

        public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest r) {
            return votes == null ? new CompletableFuture<>() : CompletableFuture.completedFuture(
                    RequestVoteResponse.newBuilder().setTerm(r.getTerm()).setVoteGranted(votes).build());
        }

        public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest r) {
            return votes == null ? new CompletableFuture<>() : CompletableFuture.completedFuture(
                    PreVoteResponse.newBuilder().setTerm(r.getTerm() - 1).setVoteGranted(votes).build());
        }

        public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest r) {
            return append == null ? new CompletableFuture<>() : CompletableFuture.completedFuture(
                    AppendEntriesResponse.newBuilder().setTerm(r.getTerm()).setSuccess(append).build());
        }

        public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest r) {
            return new CompletableFuture<>();
        }

        public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest r) {
            return new CompletableFuture<>();
        }

        public void close() { }
    }

    private static final class Delegating implements RaftTransport {
        final RaftNode target;

        Delegating(RaftNode target) {
            this.target = target;
        }

        public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest r) {
            return CompletableFuture.supplyAsync(() -> target.handleRequestVote(r));
        }

        public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest r) {
            return CompletableFuture.supplyAsync(() -> target.handlePreVote(r));
        }

        public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest r) {
            return CompletableFuture.supplyAsync(() -> target.handleAppendEntries(r));
        }

        public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest r) {
            return CompletableFuture.supplyAsync(() -> target.handleInstallSnapshot(r));
        }

        public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest r) {
            return CompletableFuture.supplyAsync(() -> target.handleTimeoutNow(r));
        }

        public void close() { }
    }
}
