/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * §8 end to end on a node: a command submitted twice under the same client id and sequence
 * takes effect once, and that stays true across the two events that lose in-memory state
 * -- a restart that replays the log, and a restart from a snapshot.
 *
 * <p>The state machine here is a {@link CountingStateMachine} on purpose. A key-value
 * {@code SET} is idempotent, so applying it twice looks exactly like applying it once and a
 * broken de-duplication passes every assertion.
 */
class ClientRequestDeduplicationTest {

    private static final byte[] INCREMENT = {1};

    private RaftNode node;

    @AfterEach
    void stop() {
        if (node != null) {
            node.shutdown();
        }
    }

    private RaftNode startLeader(RaftStorage store, StateMachine stateMachine) throws InterruptedException {
        RaftConfig config = RaftConfig.of("n1", 19801, Path.of("unused"), Map.of("n1", "localhost:19801"));
        node = new RaftNode(config, store, stateMachine, addr -> null, RaftMetrics.noop());
        node.start();
        Await.until("the single node to elect itself", 5_000, () -> node.role() == ServerRole.LEADER);
        return node;
    }

    private static byte[] submit(RaftNode node, String clientId, long sequence, long acked) throws Exception {
        return node.submitCommand(INCREMENT, clientId, sequence, acked).get(5, TimeUnit.SECONDS);
    }

    @Test
    void aRetriedCommandIsAppliedOnceAndAnsweredWithTheFirstResult() throws Exception {
        CountingStateMachine counter = new CountingStateMachine();
        RaftNode leader = startLeader(new InMemoryStorage(), counter);

        byte[] first = submit(leader, "client-a", 1, 0);
        byte[] retry = submit(leader, "client-a", 1, 0);

        assertEquals(1, counter.value(), "the retry must not reach the state machine");
        assertArrayEquals(first, retry, "the retry is answered with what the first application returned");
    }

    @Test
    void aCommandWithoutASessionIsAppliedOnEveryArrival() throws Exception {
        CountingStateMachine counter = new CountingStateMachine();
        RaftNode leader = startLeader(new InMemoryStorage(), counter);

        leader.submitCommand(INCREMENT).get(5, TimeUnit.SECONDS);
        leader.submitCommand(INCREMENT).get(5, TimeUnit.SECONDS);

        assertEquals(2, counter.value(), "untagged commands keep the behaviour they always had");
    }

    @Test
    void differentClientsAndDifferentSequencesAreDifferentCommands() throws Exception {
        CountingStateMachine counter = new CountingStateMachine();
        RaftNode leader = startLeader(new InMemoryStorage(), counter);

        submit(leader, "client-a", 1, 0);
        submit(leader, "client-a", 2, 1);
        submit(leader, "client-b", 1, 0);

        assertEquals(3, counter.value());
        assertEquals(2, leader.clientSessionCount());
    }

    @Test
    void anAlreadyAcknowledgedSequenceIsRefusedNotReapplied() throws Exception {
        CountingStateMachine counter = new CountingStateMachine();
        RaftNode leader = startLeader(new InMemoryStorage(), counter);
        submit(leader, "client-a", 1, 0);
        submit(leader, "client-a", 2, 1); // acknowledges 1

        ExecutionException refused = assertThrows(ExecutionException.class,
                () -> leader.submitCommand(INCREMENT, "client-a", 1, 0).get(5, TimeUnit.SECONDS));

        assertInstanceOf(RaftNode.StaleClientRequestException.class, refused.getCause());
        assertEquals(2, counter.value());
    }

    @Test
    void aMalformedTagIsRejectedBeforeItReachesTheLog() throws Exception {
        RaftNode leader = startLeader(new InMemoryStorage(), new CountingStateMachine());
        long logEnd = leader.commitIndex();

        assertThrows(ExecutionException.class,
                () -> leader.submitCommand(INCREMENT, "", 1, 0).get(1, TimeUnit.SECONDS));
        assertThrows(ExecutionException.class,
                () -> leader.submitCommand(INCREMENT, "client-a", 0, 0).get(1, TimeUnit.SECONDS));
        assertThrows(ExecutionException.class,
                () -> leader.submitCommand(INCREMENT, "client-a", 3, 3).get(1, TimeUnit.SECONDS));
        assertEquals(logEnd, leader.commitIndex());
    }

    @Test
    void theRetryIsStillRecognisedAfterARestartThatReplaysTheLog() throws Exception {
        InMemoryStorage store = new InMemoryStorage(); // outlives the node, like a disk
        byte[] first = submit(startLeader(store, new CountingStateMachine()), "client-a", 1, 0);
        node.shutdown();

        // A fresh state machine and a fresh session table, both rebuilt from the same log.
        CountingStateMachine replayed = new CountingStateMachine();
        RaftNode restarted = startLeader(store, replayed);
        byte[] retry = submit(restarted, "client-a", 1, 0);

        assertEquals(1, replayed.value());
        assertArrayEquals(first, retry);
    }

    @Test
    void theRetryIsStillRecognisedAfterARestartFromASnapshot() throws Exception {
        InMemoryStorage store = new InMemoryStorage();
        RaftNode leader = startLeader(store, new CountingStateMachine());
        byte[] first = submit(leader, "client-a", 1, 0);
        leader.snapshotNow(); // the log entry that carried the tag is gone after this
        assertEquals(leader.appliedIndex(), store.getSnapshotIndex());
        node.shutdown();

        CountingStateMachine restored = new CountingStateMachine();
        RaftNode restarted = startLeader(store, restored);
        byte[] retry = submit(restarted, "client-a", 1, 0);

        assertEquals(1, restored.value(),
                "the session table must travel in the snapshot, or the compacted command is applied again");
        assertArrayEquals(first, retry);
    }

    @Test
    void theSessionTableSurvivesTheSnapshotWireFormat() {
        byte[] sm = {1, 2, 3};
        byte[] cfg = {4, 5};
        byte[] sessions = {6, 7, 8, 9};

        RaftStorage.Snapshot unpacked = RaftNode.unpackSnapshotData(7, 2,
                RaftNode.packSnapshotData(sm, cfg, sessions));

        assertArrayEquals(sm, unpacked.stateMachineData);
        assertArrayEquals(cfg, unpacked.configurationData);
        assertArrayEquals(sessions, unpacked.sessionData);
    }

    @Test
    void withoutSessionsTheSnapshotWireFormatIsTheOneOlderNodesRead() {
        byte[] sm = {1, 2, 3};
        byte[] cfg = {4, 5};

        byte[] packed = RaftNode.packSnapshotData(sm, cfg, new byte[0]);

        assertArrayEquals(RaftNode.packSnapshotData(sm, cfg), packed);
        assertArrayEquals(new byte[] {0, 0, 0, 3, 1, 2, 3, 4, 5}, packed);
        assertEquals(0, RaftNode.unpackSnapshotData(7, 2, packed).sessionData.length);
    }
}
