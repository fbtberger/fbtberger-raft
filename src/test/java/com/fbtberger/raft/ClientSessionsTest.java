/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** The session table on its own: what counts as a duplicate, what is forgotten, and in which order. */
class ClientSessionsTest {

    private static byte[] bytes(String s) {
        return s.getBytes();
    }

    @Test
    void aCommandSeenForTheFirstTimeIsNew() {
        ClientSessions sessions = new ClientSessions();

        assertEquals(ClientSessions.Kind.NEW, sessions.begin("c1", 1, 0).kind());
    }

    @Test
    void aRepeatIsAnsweredWithTheRecordedResult() {
        ClientSessions sessions = new ClientSessions();
        sessions.begin("c1", 1, 0);
        sessions.record("c1", 1, bytes("first"));

        ClientSessions.Lookup repeat = sessions.begin("c1", 1, 0);

        assertEquals(ClientSessions.Kind.DUPLICATE, repeat.kind());
        assertArrayEquals(bytes("first"), repeat.result());
    }

    @Test
    void theSameSequenceFromAnotherClientIsADifferentCommand() {
        ClientSessions sessions = new ClientSessions();
        sessions.begin("c1", 1, 0);
        sessions.record("c1", 1, bytes("first"));

        assertEquals(ClientSessions.Kind.NEW, sessions.begin("c2", 1, 0).kind());
    }

    @Test
    void aCommandThatArrivesOutOfOrderIsStillNew() {
        // Two threads sharing one client can commit 2 before 1. A table that only kept
        // "the highest sequence seen" would call 1 a duplicate and drop a real command.
        ClientSessions sessions = new ClientSessions();
        sessions.begin("c1", 2, 0);
        sessions.record("c1", 2, bytes("two"));

        assertEquals(ClientSessions.Kind.NEW, sessions.begin("c1", 1, 0).kind());
    }

    @Test
    void anAcknowledgedResultIsDroppedAndItsRepeatRefused() {
        ClientSessions sessions = new ClientSessions();
        sessions.begin("c1", 1, 0);
        sessions.record("c1", 1, bytes("one"));
        sessions.begin("c1", 2, 1); // "I have the answer to 1"
        sessions.record("c1", 2, bytes("two"));

        assertEquals(ClientSessions.Kind.STALE, sessions.begin("c1", 1, 1).kind(),
                "an acknowledged command must be neither re-applied nor answered from a result that is gone");
        assertEquals(ClientSessions.Kind.DUPLICATE, sessions.begin("c1", 2, 1).kind());
    }

    @Test
    void aNullResultIsRecordedAsEmptySoTheRepeatIsStillRecognised() {
        ClientSessions sessions = new ClientSessions();
        sessions.begin("c1", 1, 0);
        sessions.record("c1", 1, null);

        ClientSessions.Lookup repeat = sessions.begin("c1", 1, 0);

        assertEquals(ClientSessions.Kind.DUPLICATE, repeat.kind());
        assertEquals(0, repeat.result().length);
    }

    @Test
    void theLeastRecentlyUsedClientIsForgottenFirst() {
        ClientSessions sessions = new ClientSessions(2);
        sessions.begin("old", 1, 0);
        sessions.record("old", 1, bytes("o"));
        sessions.begin("kept", 1, 0);
        sessions.record("kept", 1, bytes("k"));
        sessions.begin("old", 2, 0);       // "old" is now the more recently used of the two
        sessions.record("old", 2, bytes("o2"));

        sessions.begin("new", 1, 0);       // third client: one has to go

        assertEquals(2, sessions.size());
        assertEquals(ClientSessions.Kind.DUPLICATE, sessions.begin("old", 1, 0).kind());
        assertEquals(ClientSessions.Kind.NEW, sessions.begin("kept", 1, 0).kind(),
                "the least recently used session is the one evicted");
    }

    @Test
    void aSnapshotCarriesResultsAcknowledgementsAndTheEvictionOrder() {
        ClientSessions original = new ClientSessions(2);
        original.begin("a", 1, 0);
        original.record("a", 1, bytes("a1"));
        original.begin("b", 3, 2);
        original.record("b", 3, bytes("b3"));
        original.begin("a", 2, 0);         // order is now b, a
        original.record("a", 2, bytes("a2"));

        ClientSessions restored = new ClientSessions(2);
        restored.restore(original.toBytes());

        assertArrayEquals(original.toBytes(), restored.toBytes());
        // Looking a client up counts as using it, so both tables are asked alike.
        assertEquals(ClientSessions.Kind.STALE, original.begin("b", 2, 2).kind());
        assertEquals(ClientSessions.Kind.STALE, restored.begin("b", 2, 2).kind());
        // Both tables must now evict the same session, or two servers restored from the
        // same snapshot would disagree about the next retry.
        original.begin("c", 1, 0);
        restored.begin("c", 1, 0);
        assertArrayEquals(original.toBytes(), restored.toBytes());
    }

    @Test
    void restoringASnapshotFromBeforeSessionsGivesAnEmptyTable() {
        ClientSessions sessions = new ClientSessions();
        sessions.begin("c1", 1, 0);

        sessions.restore(new byte[0]);

        assertEquals(0, sessions.size());
    }
}
