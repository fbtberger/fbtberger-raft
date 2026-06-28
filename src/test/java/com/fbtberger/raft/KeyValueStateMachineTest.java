/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link KeyValueStateMachine}: apply logic, snapshot
 * serialisation round-trip, and restore-overwrites-current-state.
 */
class KeyValueStateMachineTest {

    private KeyValueStateMachine sm;

    @BeforeEach
    void setUp() {
        sm = new KeyValueStateMachine();
    }

    // ---- apply ----------------------------------------------------------

    @Test
    void setCommandStoresValueAndReturnsOk() {
        byte[] result = sm.apply("SET foo bar".getBytes(StandardCharsets.UTF_8));
        assertEquals("OK", new String(result, StandardCharsets.UTF_8));
        assertEquals("bar", sm.get("foo"));
    }

    @Test
    void setCommandOverwritesExistingValue() {
        sm.apply("SET k v1".getBytes(StandardCharsets.UTF_8));
        sm.apply("SET k v2".getBytes(StandardCharsets.UTF_8));
        assertEquals("v2", sm.get("k"));
    }

    @Test
    void setCommandValueMayContainSpaces() {
        sm.apply("SET greeting hello world".getBytes(StandardCharsets.UTF_8));
        assertEquals("hello world", sm.get("greeting"));
    }

    @Test
    void getForAbsentKeyReturnsNull() {
        assertNull(sm.get("nosuchkey"));
    }

    @Test
    void emptyCommandNoOpReturnsEmptyBytes() {
        byte[] result = sm.apply(new byte[0]);
        assertEquals(0, result.length);
    }

    @Test
    void unknownCommandReturnsErrPrefix() {
        byte[] result = sm.apply("DELETE foo".getBytes(StandardCharsets.UTF_8));
        assertTrue(new String(result, StandardCharsets.UTF_8).startsWith("ERR"));
    }

    // ---- snapshot round-trip -------------------------------------------

    @Test
    void snapshotRoundTripPreservesAllEntries() {
        sm.apply("SET a 1".getBytes(StandardCharsets.UTF_8));
        sm.apply("SET b 2".getBytes(StandardCharsets.UTF_8));
        sm.apply("SET c 3".getBytes(StandardCharsets.UTF_8));

        byte[] snapshot = sm.takeSnapshot();

        KeyValueStateMachine fresh = new KeyValueStateMachine();
        fresh.restoreSnapshot(snapshot);

        assertEquals("1", fresh.get("a"));
        assertEquals("2", fresh.get("b"));
        assertEquals("3", fresh.get("c"));
    }

    @Test
    void snapshotOfEmptyMachineRoundTrips() {
        byte[] snapshot = sm.takeSnapshot();
        KeyValueStateMachine fresh = new KeyValueStateMachine();
        fresh.restoreSnapshot(snapshot);
        assertNull(fresh.get("anything"));
    }

    @Test
    void restoreOverwritesExistingState() {
        sm.apply("SET old value".getBytes(StandardCharsets.UTF_8));
        byte[] snapshot = sm.takeSnapshot();

        KeyValueStateMachine other = new KeyValueStateMachine();
        other.apply("SET old DIFFERENT".getBytes(StandardCharsets.UTF_8));
        other.apply("SET extra key".getBytes(StandardCharsets.UTF_8));

        other.restoreSnapshot(snapshot);

        assertEquals("value", other.get("old"));
        assertNull(other.get("extra")); // must have been wiped by restore
    }

    @Test
    void restoreWithCorruptDataThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> sm.restoreSnapshot(new byte[]{0, 0, 0, 10, 'x'})); // count=10 but no data
    }
}
