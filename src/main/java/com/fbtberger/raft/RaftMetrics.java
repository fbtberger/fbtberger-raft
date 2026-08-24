/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public final class RaftMetrics {

        private static final RaftMetrics NOOP = new RaftMetrics(new SimpleMeterRegistry(), "noop");

        private final MeterRegistry registry;
        private final Tags tags;

        /** One entry per peer the leader is replicating to. Empty on a follower. */
        private final Map<String, PeerMeters> peerMeters = new ConcurrentHashMap<>();

        private final Counter electionsStarted;
        private final Counter electionsWon;
        private final Counter votesGranted;
        private final Counter snapshotsTaken;
        private final Counter snapshotsInstalled;
        private final Counter entriesApplied;
        private final Counter stepDowns;
        private final Counter clientRejected;
        private final Counter replicationSent;
        private final Counter snapshotChunksSent;
        private final Counter snapshotChunksReceived;
        private final Counter replicationSuccess;
        private final Counter replicationFailure;

        private final Timer appendEntriesTimer;
        private final Timer requestVoteTimer;
        private final Timer installSnapshotTimer;
        private final Timer clientSubmitTimer;

        public RaftMetrics(MeterRegistry registry, String nodeId) {
                this.registry = registry;
                this.tags = Tags.of("node", nodeId);

                this.electionsStarted = Counter.builder("raft.election.started")
                                .tags(tags).description("Elections started by this node").register(registry);
                this.electionsWon = Counter.builder("raft.election.won")
                                .tags(tags).description("Elections won by this node").register(registry);
                this.votesGranted = Counter.builder("raft.vote.granted")
                                .tags(tags).description("Votes granted to other candidates").register(registry);
                this.snapshotsTaken = Counter.builder("raft.snapshot.taken")
                                .tags(tags).description("Snapshots taken").register(registry);
                this.snapshotsInstalled = Counter.builder("raft.snapshot.installed")
                                .tags(tags).description("Snapshots installed from leader").register(registry);
                this.entriesApplied = Counter.builder("raft.entry.applied")
                                .tags(tags).description("Log entries applied to state machine").register(registry);
                this.stepDowns = Counter.builder("raft.stepdown")
                                .tags(tags).description("Times this node stepped down to follower").register(registry);
                this.clientRejected = Counter.builder("raft.client.rejected")
                                .tags(tags).description("Client submissions rejected (not leader)").register(registry);
                this.replicationSent = Counter.builder("raft.replication.sent")
                                .tags(tags).description("Log entries sent to followers").register(registry);
                this.snapshotChunksSent = Counter.builder("raft.snapshot.chunk.sent")
                                .tags(tags).description("Snapshot chunks sent to followers").register(registry);
                this.snapshotChunksReceived = Counter.builder("raft.snapshot.chunk.received")
                                .tags(tags).description("Snapshot chunks received from leader").register(registry);
                this.replicationSuccess = Counter.builder("raft.replication.success")
                                .tags(tags).description("Successful AppendEntries responses from followers").register(registry);
                this.replicationFailure = Counter.builder("raft.replication.failure")
                                .tags(tags).description("Failed AppendEntries responses (log mismatch backoff)").register(registry);

                this.appendEntriesTimer = Timer.builder("raft.rpc.append.entries")
                                .tags(tags).description("AppendEntries RPC handling time").register(registry);
                this.requestVoteTimer = Timer.builder("raft.rpc.request.vote")
                                .tags(tags).description("RequestVote RPC handling time").register(registry);
                this.installSnapshotTimer = Timer.builder("raft.rpc.install.snapshot")
                                .tags(tags).description("InstallSnapshot RPC handling time").register(registry);
                this.clientSubmitTimer = Timer.builder("raft.client.submit")
                                .tags(tags).description("Client command submit-to-commit time").register(registry);
        }

        public static RaftMetrics noop() {
                return NOOP;
        }

        public void registerGauges(Supplier<Number> term,
                        Supplier<Number> commitIndex,
                        Supplier<Number> lastApplied,
                        Supplier<Number> role,
                        Supplier<Number> clusterSize,
                        Supplier<Number> logLastIndex,
                        Supplier<Number> snapshotIndex) {
                Gauge.builder("raft.node.term", term).tags(tags)
                                .description("Current Raft term").register(registry);
                Gauge.builder("raft.node.commit.index", commitIndex).tags(tags)
                                .description("Highest committed log index").register(registry);
                Gauge.builder("raft.node.last.applied", lastApplied).tags(tags)
                                .description("Last log index applied to state machine").register(registry);
                Gauge.builder("raft.node.role", role).tags(tags)
                                .description("Current role (0=follower, 1=candidate, 2=leader)").register(registry);
                Gauge.builder("raft.cluster.size", clusterSize).tags(tags)
                                .description("Number of nodes in current configuration").register(registry);
                Gauge.builder("raft.node.log.last.index", logLastIndex).tags(tags)
                                .description("Index of the last log entry").register(registry);
                Gauge.builder("raft.node.snapshot.index", snapshotIndex).tags(tags)
                                .description("Snapshot boundary (last compacted index)").register(registry);
        }

        /**
         * How far each peer has got, as the leader sees it.
         *
         * <p>The leader already knows this -- it logs a line per peer every ten seconds --
         * and until now that was the only way out. A log line is not an interface: reading
         * it means an SSH session and a parser coupled to a format nobody declared stable.
         *
         * <p>The series carry a {@code peer} and a {@code peer_role} tag, so the set of
         * series on the leader <em>is</em> the live configuration, roles included. That is
         * something no other metric says: {@code raft.cluster.size} gives a count, and a
         * node removed from the configuration keeps reporting the last count it knew.
         *
         * <p>Only a leader has any of this. A follower publishes nothing here, and a leader
         * that steps down drops what it published -- see {@link #forgetPeersExcept}. A
         * stale series claiming a node is still being replicated to would be worse than no
         * series at all.
         */
        public void peerReplication(String peerId, String peerRole, long matchIndex, long lastAckAgeMillis) {
                PeerMeters existing = peerMeters.get(peerId);
                if (existing != null && !existing.role.equals(peerRole)) {
                        // A learner that has been promoted is the same peer with a different
                        // tag, and Micrometer keys a meter by name plus tags. Leaving the old
                        // one would publish the node twice, once under each role.
                        removePeerMeters(peerId, existing.role);
                        existing = null;
                }
                if (existing == null) {
                        existing = new PeerMeters(peerRole);
                        Tags peerTags = tags.and("peer", peerId).and("peer_role", peerRole);
                        Gauge.builder("raft.replication.match.index", existing.matchIndex, AtomicLong::doubleValue)
                                        .tags(peerTags)
                                        .description("Highest log index this peer has acknowledged, per the leader")
                                        .register(registry);
                        Gauge.builder("raft.replication.last.ack.seconds", existing.lastAckAgeMillis,
                                        a -> a.get() < 0 ? Double.NaN : a.get() / 1000.0)
                                        .tags(peerTags)
                                        .description("Seconds since this peer last acknowledged anything")
                                        .register(registry);
                        peerMeters.put(peerId, existing);
                }
                existing.matchIndex.set(matchIndex);
                existing.lastAckAgeMillis.set(lastAckAgeMillis);
        }

        /**
         * Drops the peers that are no longer in the configuration.
         *
         * <p>Called with the whole membership every time it is published, so removal needs
         * no separate notification and cannot be forgotten on a path that removes a node.
         * Passing an empty collection clears everything, which is what a node that has just
         * stepped down should do.
         */
        public void forgetPeersExcept(Collection<String> stillMembers) {
                peerMeters.keySet().removeIf(peerId -> {
                        if (stillMembers.contains(peerId)) {
                                return false;
                        }
                        removePeerMeters(peerId, peerMeters.get(peerId).role);
                        return true;
                });
        }

        private void removePeerMeters(String peerId, String peerRole) {
                Tags peerTags = tags.and("peer", peerId).and("peer_role", peerRole);
                registry.find("raft.replication.match.index").tags(peerTags).gauges()
                                .forEach(registry::remove);
                registry.find("raft.replication.last.ack.seconds").tags(peerTags).gauges()
                                .forEach(registry::remove);
        }

        private static final class PeerMeters {
                private final String role;
                private final AtomicLong matchIndex = new AtomicLong();
                private final AtomicLong lastAckAgeMillis = new AtomicLong(-1);

                private PeerMeters(String role) {
                        this.role = role;
                }
        }

        public void electionStarted() {
                electionsStarted.increment();
        }

        public void electionWon() {
                electionsWon.increment();
        }

        public void voteGranted() {
                votesGranted.increment();
        }

        public void snapshotTaken() {
                snapshotsTaken.increment();
        }

        public void snapshotInstalled() {
                snapshotsInstalled.increment();
        }

        public void entryApplied() {
                entriesApplied.increment();
        }

        public void stepDown() {
                stepDowns.increment();
        }

        public void clientRejected() {
                clientRejected.increment();
        }

        public void replicationSent(int entryCount) {
                replicationSent.increment(entryCount);
        }

        public void snapshotChunkSent() {
                snapshotChunksSent.increment();
        }

        public void snapshotChunkReceived() {
                snapshotChunksReceived.increment();
        }

        public void replicationSuccess() {
                replicationSuccess.increment();
        }

        public void replicationFailure() {
                replicationFailure.increment();
        }

        public Timer appendEntriesTimer() {
                return appendEntriesTimer;
        }

        public Timer requestVoteTimer() {
                return requestVoteTimer;
        }

        public Timer installSnapshotTimer() {
                return installSnapshotTimer;
        }

        public Timer clientSubmitTimer() {
                return clientSubmitTimer;
        }

        public MeterRegistry registry() {
                return registry;
        }
}
