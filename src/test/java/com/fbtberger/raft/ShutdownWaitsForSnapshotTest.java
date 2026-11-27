/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * After {@link RaftNode#shutdown()} has returned, the node must not write to its
 * storage any more.
 *
 * <p>A snapshot is serialized and written on its own thread. Shutdown used to
 * signal that thread and return at once, so a snapshot already under way
 * finished later and compacted the log then. Seen in fbtberger-common: a node
 * restarted on the same storage restored the older snapshot, the old node's
 * snapshot thread then discarded the entries behind it, and the new node could
 * never apply them.
 */
class ShutdownWaitsForSnapshotTest {

    @Test
    void shutdownReturnsOnlyAfterARunningSnapshotHasBeenWritten() throws Exception {
        SlowSnapshotStateMachine stateMachine = new SlowSnapshotStateMachine();
        InMemoryStorage store = new InMemoryStorage();
        RaftNode node = new RaftNode(config(), store, stateMachine, addr -> null, RaftMetrics.noop());
        node.start();
        Await.until("single node becomes leader", 2_000, () -> node.role() == ServerRole.LEADER);

        node.submitCommand("a".getBytes(StandardCharsets.UTF_8)).get(2, TimeUnit.SECONDS);
        assertTrue(stateMachine.serializing.await(2, TimeUnit.SECONDS), "a snapshot is being serialized");

        Thread stopper = new Thread(node::shutdown, "stopper");
        stopper.start();
        stopper.join(300);
        boolean returnedEarly = !stopper.isAlive();
        long snapshotIndexWhileBlocked = store.getSnapshotIndex();

        stateMachine.release.countDown();
        stopper.join(5_000);

        assertFalse(returnedEarly, "shutdown returned while the snapshot was still being written");
        assertEquals(0, snapshotIndexWhileBlocked);
        assertFalse(stopper.isAlive(), "shutdown returns once the snapshot is written");
        long snapshotIndexAtReturn = store.getSnapshotIndex();
        assertTrue(snapshotIndexAtReturn > 0, "the running snapshot was completed, not dropped");

        Thread.sleep(200);
        assertEquals(snapshotIndexAtReturn, store.getSnapshotIndex(), "nothing is written after shutdown");
    }

    private static RaftConfig config() throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-test-unused/n1");
        props.setProperty("peer.n1", "localhost:9091");
        props.setProperty("snapshot.threshold", "1");
        Path tmp = Files.createTempFile("raft-shutdown-test-", ".properties");
        try (java.io.OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    /** Its snapshot serialization blocks until the test lets it go, whatever interrupts arrive. */
    private static final class SlowSnapshotStateMachine implements StateMachine {
        final CountDownLatch serializing = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        private final AtomicBoolean first = new AtomicBoolean(true);

        @Override
        public byte[] apply(byte[] command) {
            return command;
        }

        @Override
        public byte[] takeSnapshot() {
            return new byte[0];
        }

        @Override
        public Supplier<byte[]> prepareCowSnapshot() {
            if (!first.compareAndSet(true, false)) {
                return () -> new byte[0];
            }
            return () -> {
                serializing.countDown();
                boolean interrupted = false;
                while (true) {
                    try {
                        release.await();
                        break;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
                return new byte[0];
            };
        }

        @Override
        public void restoreSnapshot(byte[] snapshot) {
        }
    }
}
