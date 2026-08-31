/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * That taking a snapshot is timed, and not only counted.
 *
 * <p>The distinction is the whole point. {@code raft.snapshot.taken} says a snapshot
 * happened; it cannot tell a five-millisecond one from a two-second one, and the
 * difference between those two is whether the node was available while it ran. The work
 * shares a two-thread scheduler with the heartbeat and the election timer, so a slow
 * snapshot does not merely delay compaction -- it delays the messages a cluster decides
 * leadership with.
 *
 * <p>Found on the Pi cluster: the demo nodes carry no {@code snapshot.threshold}, so they
 * ran on the library default of 100 entries -- a snapshot every 480 ms under load, 1037 of
 * them in an hour, with the leader sending followers whole snapshots because the log was
 * compacted out from under them. The tail was p99.9 2.8 s against a median of 7 ms, and
 * nothing in the system could say what a snapshot cost, so nothing could confirm the
 * connection. This is that instrument.
 */
class SnapshotDurationMetricTest {

    private RaftNode node;

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.shutdown();
        }
    }

    @Test
    void takingASnapshotIsTimedAndNotOnlyCounted() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RaftMetrics metrics = new RaftMetrics(registry, "n1");
        node = new RaftNode(singleNodeConfig(), new InMemoryStorage(),
                new KeyValueStateMachine(), addr -> null, metrics);
        node.start();

        long deadline = System.currentTimeMillis() + 2_000;
        while (node.role() != ServerRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        node.submitCommand("SET a 1".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);

        node.snapshotNow();

        assertEquals(1, (long) metrics.takeSnapshotTimer().count(),
                "one snapshot must produce one timed sample, or the tail it causes stays invisible");
        assertTrue(metrics.takeSnapshotTimer().totalTime(TimeUnit.NANOSECONDS) > 0,
                "a snapshot that takes no measurable time was not measured");
        assertEquals(metrics.takeSnapshotTimer().count(), registry.counter("raft.snapshot.taken", "node", "n1").count(),
                "the count and the timer must describe the same events");
    }

    /**
     * The half that runs under the Raft lock is timed separately, because it is the more
     * dangerous one.
     *
     * <p>{@code StateMachine#prepareCowSnapshot} is called with the lock held. The SPI asks
     * implementations to be cheap there and permits them not to be:
     * {@code SqlCrudStateMachine} reads its whole table, deliberately, because deferring a
     * read of a mutating table is the worse mistake. Whatever it costs, heartbeats and votes
     * wait for it -- so this is the only part of taking a snapshot that can depose a leader
     * without touching a disk, and until this timer it was the only part nobody measured.
     * {@code raft.snapshot.duration} starts after it, inside the executor.
     */
    @Test
    void theLockHeldCaptureIsTimedSeparatelyFromTheWrite() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RaftMetrics metrics = new RaftMetrics(registry, "n1");
        // Threshold 1, so applying one command triggers the background path -- the only
        // one that captures under the lock. snapshotNow() takes a different route.
        node = new RaftNode(config("n1", 1), new InMemoryStorage(),
                new KeyValueStateMachine(), addr -> null, metrics);
        node.start();

        long deadline = System.currentTimeMillis() + 2_000;
        while (node.role() != ServerRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        node.submitCommand("SET a 1".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);

        deadline = System.currentTimeMillis() + 5_000;
        while (metrics.captureSnapshotTimer().count() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        // Not "exactly one": at threshold 1 the leader's own no-op entry is already enough
        // to trigger one, so how many happen is a matter of timing. What must hold is that
        // the lock-held phase is measured where it happens, and that it is never measured
        // fewer times than the write it precedes -- the write can be skipped when a newer
        // snapshot landed first, the capture cannot.
        assertTrue(metrics.captureSnapshotTimer().count() >= 1,
                "the lock-held capture was not timed at all");
        assertTrue(metrics.captureSnapshotTimer().totalTime(TimeUnit.NANOSECONDS) > 0,
                "a capture that takes no measurable time was not measured");
        assertTrue(metrics.captureSnapshotTimer().count() >= metrics.takeSnapshotTimer().count(),
                "every write was preceded by a capture, so it cannot be counted more often");
    }

    private static RaftConfig singleNodeConfig() throws Exception {
        return config("n1", 1_000_000);
    }

    private static RaftConfig config(String id, int snapshotThreshold) throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", id);
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-snapshot-metric-unused");
        props.setProperty("peer." + id, "localhost:9091");
        props.setProperty("snapshot.threshold", String.valueOf(snapshotThreshold));
        Path tmp = Files.createTempFile("raft-snapshot-metric-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }
}
