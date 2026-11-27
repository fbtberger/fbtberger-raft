/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link LeaderPreparation#prepare} is the leader's one chance to fix a
 * non-deterministic value into a command. These tests pin down the things a
 * state machine relying on it has to be able to trust: it runs on the leader, its
 * result is what the log holds and what apply sees, a failure appends nothing,
 * a server that is not the leader never runs it, and a state machine that does
 * not implement the interface is not involved at all.
 */
class LeaderPreparationTest {

    private RaftNode node;

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.shutdown();
        }
    }

    @Test
    void whatPrepareReturnsIsWhatTheLogHoldsAndWhatApplySees() throws Exception {
        RecordingStateMachine sm = new RecordingStateMachine(command -> concat(command, " @leader"));
        InMemoryStorage store = new InMemoryStorage();
        startLeader(store, sm);

        byte[] result = node.submitCommand(utf8("create order")).get(2, TimeUnit.SECONDS);

        assertEquals(List.of("create order"), sm.prepared);
        assertEquals(List.of("create order @leader"), sm.applied);
        assertEquals("create order @leader", text(result));
        assertEquals("create order @leader",
                store.getLogEntry(store.getLastLogIndex()).getCommand().toStringUtf8());
    }

    @Test
    void prepareRunsOncePerSubmission() throws Exception {
        RecordingStateMachine sm = new RecordingStateMachine(UnaryOperator.identity());
        startLeader(new InMemoryStorage(), sm);

        node.submitCommand(utf8("a")).get(2, TimeUnit.SECONDS);
        node.submitCommand(utf8("b")).get(2, TimeUnit.SECONDS);

        assertEquals(List.of("a", "b"), sm.prepared);
        assertEquals(List.of("a", "b"), sm.applied);
    }

    @Test
    void aFailingPrepareFailsTheRequestAndAppendsNothing() throws Exception {
        RecordingStateMachine sm = new RecordingStateMachine(command -> {
            throw new IllegalArgumentException("cannot prepare");
        });
        InMemoryStorage store = new InMemoryStorage();
        startLeader(store, sm);
        long lastIndexBefore = store.getLastLogIndex();

        CompletableFuture<byte[]> future = node.submitCommand(utf8("x"));

        ExecutionException failure =
                assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
        assertInstanceOf(IllegalArgumentException.class, failure.getCause());
        assertEquals(lastIndexBefore, store.getLastLogIndex());
        assertTrue(sm.applied.isEmpty());
    }

    @Test
    void aNullFromPrepareFailsTheRequestAndAppendsNothing() throws Exception {
        RecordingStateMachine sm = new RecordingStateMachine(command -> null);
        InMemoryStorage store = new InMemoryStorage();
        startLeader(store, sm);
        long lastIndexBefore = store.getLastLogIndex();

        CompletableFuture<byte[]> future = node.submitCommand(utf8("x"));

        ExecutionException failure =
                assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, failure.getCause());
        assertEquals(lastIndexBefore, store.getLastLogIndex());
    }

    @Test
    void aFollowerNeverPrepares() throws Exception {
        RecordingStateMachine sm = new RecordingStateMachine(UnaryOperator.identity());
        node = new RaftNode(config("f1", "other1", "other2"), new InMemoryStorage(), sm,
                addr -> null, RaftMetrics.noop());
        node.start();
        assertEquals(ServerRole.FOLLOWER, node.role());

        CompletableFuture<byte[]> future = node.submitCommand(utf8("x"));

        assertTrue(future.isCompletedExceptionally());
        assertTrue(sm.prepared.isEmpty());
    }

    @Test
    void aStateMachineWithoutLeaderPreparationGetsItsCommandUnchanged() throws Exception {
        KeyValueStateMachine sm = new KeyValueStateMachine();
        InMemoryStorage store = new InMemoryStorage();
        startLeader(store, sm);

        node.submitCommand(utf8("SET x 1")).get(2, TimeUnit.SECONDS);

        assertFalse(LeaderPreparation.class.isInstance(sm));
        assertEquals("SET x 1", store.getLogEntry(store.getLastLogIndex()).getCommand().toStringUtf8());
        assertEquals("1", sm.get("x"));
    }

    // ---- helpers ---------------------------------------------------------

    private void startLeader(InMemoryStorage store, StateMachine sm) throws Exception {
        node = new RaftNode(config("n1"), store, sm, addr -> null, RaftMetrics.noop());
        node.start();
        Await.until("single node becomes leader", 2_000, () -> node.role() == ServerRole.LEADER);
    }

    private static RaftConfig config(String selfId, String... others) throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", selfId);
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-test-unused/" + selfId);
        props.setProperty("peer." + selfId, "localhost:9091");
        int port = 9092;
        for (String other : others) {
            props.setProperty("peer." + other, "localhost:" + port++);
        }
        Path tmp = Files.createTempFile("raft-prepare-test-", ".properties");
        try (java.io.OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[] command, String suffix) {
        return utf8(text(command) + suffix);
    }

    /** Records what it was asked to prepare and to apply; echoes the applied command. */
    private static final class RecordingStateMachine implements StateMachine, LeaderPreparation {
        final List<String> prepared = new CopyOnWriteArrayList<>();
        final List<String> applied = new CopyOnWriteArrayList<>();
        private final UnaryOperator<byte[]> onPrepare;

        RecordingStateMachine(UnaryOperator<byte[]> onPrepare) {
            this.onPrepare = onPrepare;
        }

        @Override
        public byte[] prepare(byte[] command) {
            prepared.add(text(command));
            return onPrepare.apply(command);
        }

        @Override
        public byte[] apply(byte[] command) {
            // A new leader commits an empty no-op (§8) and it is applied like any
            // entry; it was never submitted, so it is not what these tests count.
            if (command.length > 0) {
                applied.add(text(command));
            }
            return command;
        }

        @Override
        public byte[] takeSnapshot() {
            return utf8(String.join("\n", new ArrayList<>(applied)));
        }

        @Override
        public void restoreSnapshot(byte[] snapshot) {
            applied.clear();
        }
    }
}
