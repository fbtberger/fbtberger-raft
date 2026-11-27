/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.LogEntry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * One state machine, one domain: a node that runs a named state machine beside
 * its application state machine delivers every command to exactly one of them.
 */
class NamedStateMachineTest {

    private RaftNode node;

    @AfterEach
    void tearDown() {
        if (node != null) {
            node.shutdown();
        }
    }

    @Test
    void aPlainCommandReachesOnlyTheApplicationAndANamedOneOnlyItsStateMachine() throws Exception {
        Recorder app = new Recorder();
        Recorder values = new Recorder();
        InMemoryStorage store = new InMemoryStorage();
        startLeader(store, app, values);

        byte[] plain = node.submitCommand(utf8("create order")).get(2, TimeUnit.SECONDS);
        byte[] named = node.submitCommand("values", utf8("issue 3")).get(2, TimeUnit.SECONDS);

        assertEquals(List.of("create order"), app.applied);
        assertEquals(List.of("issue 3"), values.applied);
        assertEquals("create order", text(plain));
        assertEquals("issue 3", text(named));
        assertEquals(List.of("values"), List.copyOf(node.stateMachineNames()));
    }

    @Test
    void theLogEntrySaysWhichStateMachineItIsFor() throws Exception {
        InMemoryStorage store = new InMemoryStorage();
        startLeader(store, new Recorder(), new Recorder());

        node.submitCommand(utf8("plain")).get(2, TimeUnit.SECONDS);
        LogEntry plain = store.getLogEntry(store.getLastLogIndex());
        node.submitCommand("values", utf8("named")).get(2, TimeUnit.SECONDS);
        LogEntry named = store.getLogEntry(store.getLastLogIndex());

        assertEquals(LogEntry.PayloadCase.COMMAND, plain.getPayloadCase());
        assertEquals("plain", plain.getCommand().toStringUtf8());
        assertEquals(LogEntry.PayloadCase.NAMED_COMMAND, named.getPayloadCase());
        assertEquals("values", named.getNamedCommand().getStateMachine());
        assertEquals("named", named.getNamedCommand().getCommand().toStringUtf8());
    }

    @Test
    void aCommandForAnUnknownStateMachineFailsAndAppendsNothing() throws Exception {
        Recorder app = new Recorder();
        InMemoryStorage store = new InMemoryStorage();
        startLeader(store, app, new Recorder());
        long lastIndexBefore = store.getLastLogIndex();

        CompletableFuture<byte[]> future = node.submitCommand("nobody", utf8("x"));

        ExecutionException failure =
                assertThrows(ExecutionException.class, () -> future.get(2, TimeUnit.SECONDS));
        assertInstanceOf(UnknownStateMachineException.class, failure.getCause());
        assertEquals(lastIndexBefore, store.getLastLogIndex());
        assertTrue(app.applied.isEmpty());
    }

    @Test
    void onlyTheAddressedStateMachineIsAskedToPrepare() throws Exception {
        Recorder app = new Recorder();
        PreparingRecorder values = new PreparingRecorder();
        InMemoryStorage store = new InMemoryStorage();
        startLeader(store, app, values);

        node.submitCommand(utf8("create order")).get(2, TimeUnit.SECONDS);
        assertTrue(values.prepared.isEmpty());

        node.submitCommand("values", utf8("issue")).get(2, TimeUnit.SECONDS);

        assertEquals(List.of("issue"), values.prepared);
        assertEquals(List.of("issue @leader"), values.applied);
        assertEquals(List.of("create order"), app.applied);
        assertEquals("issue @leader",
                store.getLogEntry(store.getLastLogIndex()).getNamedCommand().getCommand().toStringUtf8());
    }

    @Test
    void aQueryIsAnsweredByTheStateMachineItNames() throws Exception {
        Recorder app = new Recorder();
        Recorder values = new Recorder();
        startLeader(new InMemoryStorage(), app, values);
        node.submitCommand(utf8("a")).get(2, TimeUnit.SECONDS);
        node.submitCommand("values", utf8("b")).get(2, TimeUnit.SECONDS);

        assertEquals("a", text(node.query(utf8("?")).get(2, TimeUnit.SECONDS)));
        assertEquals("b", text(node.query("values", utf8("?")).get(2, TimeUnit.SECONDS)));
        assertEquals("b", text(node.queryWithLease("values", utf8("?")).get(2, TimeUnit.SECONDS)));
        assertTrue(node.query("nobody", utf8("?")).isCompletedExceptionally());
        assertTrue(node.queryWithLease("nobody", utf8("?")).isCompletedExceptionally());
    }

    @Test
    void aSnapshotCarriesBothStatesAndARestartGivesEachItsOwnBack() throws Exception {
        Recorder app = new Recorder();
        Recorder values = new Recorder();
        InMemoryStorage store = new InMemoryStorage();
        startLeader(store, app, values);
        node.submitCommand(utf8("order 1")).get(2, TimeUnit.SECONDS);
        node.submitCommand("values", utf8("issued 7")).get(2, TimeUnit.SECONDS);
        node.submitCommand(utf8("order 2")).get(2, TimeUnit.SECONDS);

        node.snapshotNow();
        assertTrue(StateMachineRegistry.isContainer(store.getSnapshot().stateMachineData));
        node.shutdown();

        Recorder app2 = new Recorder();
        Recorder values2 = new Recorder();
        node = new RaftNode(config("n1"), store,
                new StateMachineRegistry(app2, Map.of("values", values2)), addr -> null, RaftMetrics.noop());

        assertEquals(List.of("order 1", "order 2"), app2.applied);
        assertEquals(List.of("issued 7"), values2.applied);
    }

    @Test
    void aNodeWithoutNamedStateMachinesWritesThePlainSnapshotItAlwaysWrote() throws Exception {
        Recorder app = new Recorder();
        InMemoryStorage store = new InMemoryStorage();
        node = new RaftNode(config("n1"), store, app, addr -> null, RaftMetrics.noop());
        node.start();
        Await.until("single node becomes leader", 2_000, () -> node.role() == ServerRole.LEADER);
        node.submitCommand(utf8("order 1")).get(2, TimeUnit.SECONDS);

        node.snapshotNow();

        assertFalse(StateMachineRegistry.isContainer(store.getSnapshot().stateMachineData));
        assertEquals("order 1", text(store.getSnapshot().stateMachineData));
    }

    // ---- helpers ---------------------------------------------------------

    private void startLeader(InMemoryStorage store, StateMachine app, StateMachine values) throws Exception {
        node = new RaftNode(config("n1"), store,
                new StateMachineRegistry(app, Map.of("values", values)), addr -> null, RaftMetrics.noop());
        node.start();
        Await.until("single node becomes leader", 2_000, () -> node.role() == ServerRole.LEADER);
    }

    private static RaftConfig config(String selfId) throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", selfId);
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-test-unused/" + selfId);
        props.setProperty("peer." + selfId, "localhost:9091");
        Path tmp = Files.createTempFile("raft-named-test-", ".properties");
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

    /** Records what it applied; a read answers the last command; the snapshot is the list. */
    private static class Recorder implements StateMachine {
        final List<String> applied = new CopyOnWriteArrayList<>();

        @Override
        public byte[] apply(byte[] command) {
            // A new leader commits an empty no-op (§8); it was never submitted.
            if (command.length > 0) {
                applied.add(text(command));
            }
            return command;
        }

        @Override
        public byte[] read(byte[] query) {
            return utf8(applied.get(applied.size() - 1));
        }

        @Override
        public byte[] takeSnapshot() {
            return utf8(String.join("\n", applied));
        }

        @Override
        public void restoreSnapshot(byte[] snapshot) {
            applied.clear();
            if (snapshot.length > 0) {
                applied.addAll(List.of(text(snapshot).split("\n")));
            }
        }
    }

    private static final class PreparingRecorder extends Recorder implements LeaderPreparation {
        final List<String> prepared = new CopyOnWriteArrayList<>();

        @Override
        public byte[] prepare(byte[] command) {
            prepared.add(text(command));
            return utf8(text(command) + " @leader");
        }
    }
}
