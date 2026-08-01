/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
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
import com.fbtberger.raft.transport.RaftTransportFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * v123 -- a failed replication must nudge the peer's transport out of its reconnect backoff.
 *
 * <h2>Why</h2>
 * The requester side of issue #2: a restarting voter reached its 150-300 ms election timeout and
 * campaigned before the leader managed to reach it again, every single time. Measured on dev, the
 * leader's reconnect cadence was ~2 s -- {@code REBUILD_COOLDOWN_MS} -- because throwing the
 * channel away and building a new one was the <i>only</i> lever {@code RaftNode} had over a stuck
 * transport, and that lever is expensive enough to need throttling. Against a 150-300 ms timeout,
 * a 2 s cadence is not a race: the restarting node wins by a factor of seven or more.
 *
 * <p>{@code resetBackoff()} adds the cheap lever. It costs nothing, so it runs on every failure
 * with no cooldown, while the rebuild keeps its cooldown and its role as the thing that heals a
 * peer returning on a different address.
 *
 * <h2>Determinism</h2>
 * These tests call {@code replicateTo} directly and never make the node a leader. Replication is
 * otherwise only reachable through the heartbeat scheduler, and counting events while a 50 ms
 * timer fires alongside the assertions is flaky by construction -- the first version of this test
 * tried to piggyback on becoming leader and failed its own precondition, because
 * {@code becomeLeaderLocked} only *schedules* the heartbeat task rather than replicating inline.
 *
 * <p>The stub returns an already-failed future, so {@code whenComplete} runs inline on the calling
 * thread: everything asserted here has happened by the time {@code replicateTo} returns. No
 * sleeps, no awaits, no scheduler.
 */
class TransportBackoffResetTest {

    private static final Map<String, String> ADDRESS_OF = Map.of(
            "n1", "localhost:7401", "n2", "localhost:7402", "n3", "localhost:7403");

    private RaftNode node;
    private FailingPeers peers;

    @BeforeEach
    void boot() {
        peers = new FailingPeers();
        node = new RaftNode(configFor("n1"), new InMemoryStorage(), new KeyValueStateMachine(),
                peers, RaftMetrics.noop());
        // No start(): the background election timer would add unscheduled replication rounds and
        // make the counts below meaningless.
    }

    @AfterEach
    void tearDown() {
        if (node != null) node.shutdown();
    }

    /**
     * THE REGRESSION. A single failed replication must nudge that peer's transport to reconnect
     * immediately, instead of leaving it to sit out gRPC's backoff.
     */
    @Test
    void aFailedReplicationResetsThePeersBackoff() {
        node.replicateTo("n2");

        assertEquals(1, peers.appendFailures(), "precondition: exactly one attempt was made");
        assertEquals(1, peers.backoffResets(),
                "a failed replication must nudge the transport to reconnect");
    }

    /**
     * The reset is not throttled. That is the whole point: it has to fire in the window BEFORE the
     * rebuild's 2 s cooldown expires, which is the window a restarting peer campaigns in. Pre-fix,
     * the first two failures did nothing at all and the third was still gated by the cooldown.
     */
    @Test
    void everyFailureResetsTheBackoffNotJustTheThirdOne() {
        node.replicateTo("n2");
        node.replicateTo("n2");
        node.replicateTo("n2");

        assertEquals(3, peers.appendFailures(), "precondition: three attempts were made");
        assertEquals(3, peers.backoffResets(),
                "resetBackoff() must run once per failed AppendEntries, with no threshold and no"
                        + " cooldown");
    }

    /** The counterweight: a successful replication must not nudge anything. */
    @Test
    void aSuccessfulReplicationDoesNotResetTheBackoff() {
        peers.succeedFromNowOn();

        node.replicateTo("n2");

        assertEquals(0, peers.appendFailures(), "precondition: the attempt succeeded");
        assertEquals(0, peers.backoffResets(),
                "a healthy transport must not be nudged -- resetBackoff belongs on the error path");
    }

    // -- Harness ---------------------------------------------------------------

    private static RaftConfig configFor(String selfId) {
        try {
            java.util.Properties props = new java.util.Properties();
            props.setProperty("node.id", selfId);
            props.setProperty("node.port", "7401");
            props.setProperty("data.dir", "/tmp/raft-backoff-unused/" + selfId);
            ADDRESS_OF.forEach((id, addr) -> props.setProperty("peer." + id, addr));
            java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-backoff-", ".properties");
            try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) {
                props.store(out, null);
            }
            return RaftConfig.load(tmp);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Fails every AppendEntries (unless told otherwise), counting failures and nudges. */
    private static final class FailingPeers implements RaftTransportFactory {

        private final AtomicInteger appendFailures = new AtomicInteger();
        private final AtomicInteger backoffResets = new AtomicInteger();
        private volatile boolean failing = true;

        int appendFailures() {
            return appendFailures.get();
        }

        int backoffResets() {
            return backoffResets.get();
        }

        void succeedFromNowOn() {
            failing = false;
        }

        @Override
        public RaftTransport connect(String address) {
            return new RaftTransport() {
                @Override
                public void resetBackoff() {
                    backoffResets.incrementAndGet();
                }

                @Override
                public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest r) {
                    if (!failing) {
                        return CompletableFuture.completedFuture(AppendEntriesResponse.newBuilder()
                                .setTerm(r.getTerm()).setSuccess(true).build());
                    }
                    appendFailures.incrementAndGet();
                    return CompletableFuture.failedFuture(
                            new IllegalStateException("UNAVAILABLE: io exception (simulated)"));
                }

                @Override
                public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest r) {
                    return CompletableFuture.completedFuture(PreVoteResponse.newBuilder()
                            .setTerm(0).setVoteGranted(true).build());
                }

                @Override
                public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest r) {
                    return CompletableFuture.completedFuture(RequestVoteResponse.newBuilder()
                            .setTerm(r.getTerm()).setVoteGranted(true).build());
                }

                @Override
                public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest r) {
                    return CompletableFuture.completedFuture(
                            InstallSnapshotResponse.newBuilder().setTerm(r.getTerm()).build());
                }

                @Override
                public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest r) {
                    return CompletableFuture.completedFuture(TimeoutNowResponse.newBuilder().build());
                }

                @Override
                public void close() { }
            };
        }
    }
}
