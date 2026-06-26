package com.fbtberger.raft;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.function.Supplier;

public final class RaftMetrics {

        private static final RaftMetrics NOOP = new RaftMetrics(new SimpleMeterRegistry(), "noop");

        private final MeterRegistry registry;
        private final Tags tags;

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
