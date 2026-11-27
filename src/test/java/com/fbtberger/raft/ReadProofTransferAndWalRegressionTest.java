/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.*;
import com.fbtberger.raft.transport.RaftTransport;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The second set of findings from the review of 2026-11-27: what counts as proof of
 * leadership for a read and for the lease, what a transfer reports when the target says no,
 * and what the WAL does with a damaged older segment. Each test failed before its fix.
 */
class ReadProofTransferAndWalRegressionTest {

    private RaftNode node;

    @AfterEach
    void tearDown() {
        if (node != null) node.shutdown();
    }

    /** A: an ack of a heartbeat sent BEFORE the read arrived must not confirm the read. */
    @Test
    void readIndexIsNotConfirmedByAHeartbeatSentBeforeTheRead() throws Exception {
        Peer a = new Peer();
        Peer b = new Peer();
        InMemoryStorage store = new InMemoryStorage();
        node = leader(store, a, b);
        awaitCommit(node, 1);                      // no-op committed

        a.hold = true;
        b.hold = true;
        Held early = awaitHeld(a);                 // a heartbeat sent before the read exists
        Thread.sleep(50);

        CompletableFuture<Long> read = node.readIndex();
        assertFalse(read.isDone());
        early.ack();                               // only the EARLY heartbeat is answered
        Thread.sleep(100);
        assertFalse(read.isDone(), "read confirmed by a heartbeat that was sent before the read arrived");
    }

    /** B: an ack that took 400 ms to arrive says nothing about the last 400 ms. */
    @Test
    void leaseDoesNotStartWhenTheAckArrives() throws Exception {
        Peer a = new Peer();
        Peer b = new Peer();
        InMemoryStorage store = new InMemoryStorage();
        node = leader(store, a, b);
        awaitCommit(node, 1);

        a.hold = true;
        b.hold = true;
        Held ha = awaitHeld(a);
        Held hb = awaitHeld(b);
        Thread.sleep(400);                         // far beyond the 150 ms window
        assertFalse(node.isReadyToServe(), "sanity: no ack for 400 ms, the lease must be gone");
        ha.ack();
        hb.ack();
        Thread.sleep(20);
        assertFalse(node.isReadyToServe(),
                "lease renewed by acks of heartbeats sent 400 ms ago: the followers are long free to vote");
    }

    /** C: the target declines TimeoutNow (it follows another leader); the caller must hear that. */
    @Test
    void aDeclinedTimeoutNowIsNotReportedAsSuccess() throws Exception {
        Peer a = new Peer();
        Peer b = new Peer();
        InMemoryStorage store = new InMemoryStorage();
        node = leader(store, a, b);
        awaitCommit(node, 1);
        CompletableFuture<Void> transfer = node.transferLeadership("a");
        boolean ok;
        try {
            transfer.get(2, TimeUnit.SECONDS);
            ok = true;
        } catch (Exception e) {
            ok = false;
        }
        Thread.sleep(400);
        assertEquals(ServerRole.LEADER, node.role(), "sanity: nothing was transferred");
        assertFalse(ok, "transfer reported success although the target never campaigned");
        assertFalse(node.isTransferInProgress(), "the write block must be lifted at once");
    }

    /** C, the answering side: a node asked by someone it does not follow says so. */
    @Test
    void aNodeDeclinesTimeoutNowFromSomeoneItDoesNotFollow() throws Exception {
        node = new RaftNode(config(), new InMemoryStorage(), new KeyValueStateMachine(),
                addr -> new Peer(), RaftMetrics.noop());
        TimeoutNowResponse answer = node.handleTimeoutNow(
                TimeoutNowRequest.newBuilder().setTerm(1).setLeaderId("a").build());
        assertTrue(answer.getDeclined());
        assertEquals(ServerRole.FOLLOWER, node.role());
    }

    /** D: a damaged frame in an older segment leaves a hole in the recovered log. */
    @Test
    void walRecoveryDoesNotLeaveAHole(@TempDir File dir) throws Exception {
        WalStorage wal = new WalStorage(dir, 256);
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 1; i <= 60; i++) {
            entries.add(LogEntry.newBuilder().setIndex(i).setTerm(1)
                    .setCommand(ByteString.copyFromUtf8("SET key" + i + " value" + i)).build());
        }
        wal.appendEntries(entries);
        wal.close();
        File[] segments = dir.listFiles((d, n) -> n.startsWith("wal-"));
        java.util.Arrays.sort(segments);
        assertTrue(segments.length >= 3, "need several segments, got " + segments.length);
        try (RandomAccessFile raf = new RandomAccessFile(segments[1], "rw")) {
            raf.seek(20);
            int b = raf.read();
            raf.seek(20);
            raf.write(b ^ 0xFF);
        }
        long damagedLength = segments[1].length();
        IllegalStateException refusal = assertThrows(IllegalStateException.class, () -> new WalStorage(dir, 256));
        assertTrue(refusal.getMessage().contains(segments[1].getName()), refusal.getMessage());
        assertEquals(damagedLength, segments[1].length(), "the damaged segment must be left as it was found");
    }

    /** D, the other half: a cut-off write at the very end is still just cut off. */
    @Test
    void aTornTailOfTheLastSegmentIsStillTruncated(@TempDir File dir) throws Exception {
        WalStorage wal = new WalStorage(dir, 256);
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 1; i <= 60; i++) {
            entries.add(LogEntry.newBuilder().setIndex(i).setTerm(1)
                    .setCommand(ByteString.copyFromUtf8("SET key" + i + " value" + i)).build());
        }
        wal.appendEntries(entries);
        wal.close();
        File[] segments = dir.listFiles((d, n) -> n.startsWith("wal-"));
        java.util.Arrays.sort(segments);
        File lastSegment = segments[segments.length - 1];
        try (RandomAccessFile raf = new RandomAccessFile(lastSegment, "rw")) {
            raf.setLength(raf.length() - 3);
        }
        WalStorage reopened = new WalStorage(dir, 256);
        try {
            assertEquals(59, reopened.getLastLogIndex());
            for (long i = 1; i <= 59; i++) assertNotNull(reopened.getLogEntry(i), "entry " + i);
        } finally {
            reopened.close();
        }
    }

    // ------------------------------------------------------------------

    private static RaftNode leader(InMemoryStorage store, Peer a, Peer b) throws Exception {
        RaftNode n = new RaftNode(config(), store, new KeyValueStateMachine(),
                addr -> addr.contains("-a:") ? a : b, RaftMetrics.noop());
        n.start();
        long deadline = System.currentTimeMillis() + 5_000;
        while (n.role() != ServerRole.LEADER && System.currentTimeMillis() < deadline) Thread.sleep(10);
        assertEquals(ServerRole.LEADER, n.role());
        return n;
    }

    private static RaftConfig config() throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-review2-unused");
        props.setProperty("peer.n1", "h-n1:1");
        props.setProperty("peer.a", "h-a:1");
        props.setProperty("peer.b", "h-b:1");
        props.setProperty("snapshot.threshold", "1000000");
        Path tmp = Files.createTempFile("raft-review2-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    private static void awaitCommit(RaftNode n, long index) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3_000;
        while (n.commitIndex() < index && System.currentTimeMillis() < deadline) Thread.sleep(5);
        assertTrue(n.commitIndex() >= index);
    }

    private static Held awaitHeld(Peer p) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3_000;
        Held h;
        while ((h = p.held.poll()) == null && System.currentTimeMillis() < deadline) Thread.sleep(2);
        assertNotNull(h);
        return h;
    }

    private record Held(AppendEntriesRequest request, CompletableFuture<AppendEntriesResponse> future) {
        void ack() {
            future.complete(AppendEntriesResponse.newBuilder().setTerm(request.getTerm()).setSuccess(true).build());
        }
    }

    private static final class Peer implements RaftTransport {
        volatile boolean hold;
        final ConcurrentLinkedQueue<Held> held = new ConcurrentLinkedQueue<>();

        public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest r) {
            if (hold) {
                Held h = new Held(r, new CompletableFuture<>());
                held.add(h);
                return h.future;
            }
            return CompletableFuture.completedFuture(
                    AppendEntriesResponse.newBuilder().setTerm(r.getTerm()).setSuccess(true).build());
        }

        public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest r) {
            return CompletableFuture.completedFuture(
                    RequestVoteResponse.newBuilder().setTerm(r.getTerm()).setVoteGranted(true).build());
        }

        public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest r) {
            return CompletableFuture.completedFuture(
                    PreVoteResponse.newBuilder().setTerm(r.getTerm() - 1).setVoteGranted(true).build());
        }

        public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest r) {
            return new CompletableFuture<>();
        }

        /** Answers like a node that follows someone else. */
        public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest r) {
            return CompletableFuture.completedFuture(TimeoutNowResponse.newBuilder().setDeclined(true).build());
        }

        public void close() { }
    }
}
