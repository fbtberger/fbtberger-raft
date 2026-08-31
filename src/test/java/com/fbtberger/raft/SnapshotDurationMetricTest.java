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

    private static RaftConfig singleNodeConfig() throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-snapshot-metric-unused");
        props.setProperty("peer.n1", "localhost:9091");
        // High, so the only snapshot in this test is the one it asks for.
        props.setProperty("snapshot.threshold", "1000000");
        Path tmp = Files.createTempFile("raft-snapshot-metric-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }
}
