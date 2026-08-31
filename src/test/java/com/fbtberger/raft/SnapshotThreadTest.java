/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * That a snapshot does not run on the thread pool the election timers live in.
 *
 * <p>The scheduler has two threads and four other users -- the election timer, the
 * heartbeat, the read barrier and the leadership-transfer timeout -- all of them short and
 * bounded. Taking a snapshot is the only work in {@link RaftNode} with no bound: it
 * serialises whatever the state machine holds and writes it to disk. Sharing those two
 * threads with it means the tasks a cluster settles leadership with queue behind an
 * operation whose duration is a property of the deployment, not of the protocol.
 *
 * <p>Asserted by the thread the state machine is actually called on, not by timing.
 * A timing test would need a snapshot slow enough to starve the pool, which is the
 * failure this prevents rather than a thing to reproduce -- and it would pass on a fast
 * machine either way.
 *
 * <p>This is the same shape as a fault this repository already carries a note about: an
 * applier slowed by per-call JDBC connects starved the very timers elections are decided
 * by, and the cluster elected five leaders in five seconds. Copy-on-write moved
 * serialisation off the Raft lock, which makes the remaining cost easy to believe is
 * contained. It is not the lock that decides elections.
 */
class SnapshotThreadTest {

    private RaftNode node;

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.shutdown();
        }
    }

    @Test
    void aSnapshotRunsOnItsOwnThreadAndNotOnTheSchedulers() throws Exception {
        AtomicReference<String> snapshotThread = new AtomicReference<>();
        CountDownLatch taken = new CountDownLatch(1);

        // Threshold 1, so the first applied command is enough to trigger one.
        node = new RaftNode(config("n1", 1), new InMemoryStorage(),
                new ThreadRecordingStateMachine(snapshotThread, taken), addr -> null, RaftMetrics.noop());
        node.start();

        long deadline = System.currentTimeMillis() + 2_000;
        while (node.role() != ServerRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        node.submitCommand("SET a 1".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);

        assertTrue(taken.await(5, TimeUnit.SECONDS), "no snapshot was taken");

        assertEquals("raft-snapshot-n1", snapshotThread.get(),
                "the snapshot must run on the executor reserved for it");
        assertNotEquals("raft-n1", snapshotThread.get(),
                "that is the scheduler the election timer and the heartbeat run on");
    }

    /** Records which thread the copy-on-write capture is serialised on. */
    private static final class ThreadRecordingStateMachine implements StateMachine {

        private final AtomicReference<String> thread;
        private final CountDownLatch taken;

        ThreadRecordingStateMachine(AtomicReference<String> thread, CountDownLatch taken) {
            this.thread = thread;
            this.taken = taken;
        }

        @Override
        public byte[] apply(byte[] command) {
            return new byte[0];
        }

        @Override
        public byte[] takeSnapshot() {
            return new byte[0];
        }

        @Override
        public Supplier<byte[]> prepareCowSnapshot() {
            // Deliberately records inside the supplier, not here: prepareCowSnapshot is
            // called under the Raft lock and is meant to be trivial, and the supplier is
            // the part that runs on the thread this test is about.
            return () -> {
                thread.set(Thread.currentThread().getName());
                taken.countDown();
                return new byte[0];
            };
        }

        @Override
        public void restoreSnapshot(byte[] snapshot) {
        }
    }

    private static RaftConfig config(String id, int snapshotThreshold) throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", id);
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-snapshot-thread-unused");
        props.setProperty("peer." + id, "localhost:9091");
        props.setProperty("snapshot.threshold", String.valueOf(snapshotThreshold));
        Path tmp = Files.createTempFile("raft-snapshot-thread-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }
}
