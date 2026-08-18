/*
 * Copyright 2026 fbtBerger Technology
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
    void readsBackWhatWasApplied() {
        sm.apply("SET greeting hello".getBytes(StandardCharsets.UTF_8));

        assertEquals("hello", read("GET greeting"));
    }

    /**
     * "Not there" must be distinguishable from "there and empty", otherwise a read
     * test can pass against a state machine that lost the write.
     */
    @Test
    void readOfAMissingKeyIsNotAnEmptyValue() {
        sm.apply("SET present ".getBytes(StandardCharsets.UTF_8));

        assertEquals("", read("GET present"));
        assertEquals("ERR no such key", read("GET absent"));
    }

    @Test
    void refusesAQueryItDoesNotUnderstand() {
        assertEquals("ERR unknown query: DROP TABLE", read("DROP TABLE"));
    }

    /** A read must never change the state: it runs outside the log, so a change here exists on one server only. */
    @Test
    void readDoesNotMutate() {
        sm.apply("SET k v".getBytes(StandardCharsets.UTF_8));
        byte[] before = sm.takeSnapshot();

        read("GET k");
        read("GET missing");

        assertArrayEquals(before, sm.takeSnapshot());
    }

    /** A state machine with no read side must say so rather than answer something empty. */
    @Test
    void theDefaultStateMachineRefusesReads() {
        StateMachine noReads = new StateMachine() {
            @Override
            public byte[] apply(byte[] command) {
                return new byte[0];
            }

            @Override
            public byte[] takeSnapshot() {
                return new byte[0];
            }

            @Override
            public void restoreSnapshot(byte[] snapshot) {
                // nothing to restore
            }
        };

        assertThrows(UnsupportedOperationException.class, () -> noReads.read("GET x".getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * The indexed overload must reach the same state machine. In-memory
     * implementations ignore the index by design -- they start empty, so a replay
     * rebuilds exactly the right state -- but the call has to arrive.
     */
    @Test
    void theIndexedApplyDelegatesToTheCommandApply() {
        sm.apply(7L, "SET k v".getBytes(StandardCharsets.UTF_8));

        assertEquals("v", sm.get("k"));
    }

    /**
     * The distinction the index exists for: a durable state machine sees the same
     * index twice after a restart and must not act on it twice.
     */
    @Test
    void aDurableStateMachineCanSkipAnIndexItHasAlreadyApplied() {
        class DurableCounter implements StateMachine {
            private long appliedIndex;
            private int applications;

            @Override
            public byte[] apply(long index, byte[] command) {
                if (index <= appliedIndex) {
                    return "SKIPPED".getBytes(StandardCharsets.UTF_8);
                }
                appliedIndex = index;
                applications++;
                return apply(command);
            }

            @Override
            public byte[] apply(byte[] command) {
                return "OK".getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public byte[] takeSnapshot() {
                return new byte[0];
            }

            @Override
            public void restoreSnapshot(byte[] snapshot) {
                // nothing to restore
            }
        }

        DurableCounter durable = new DurableCounter();
        byte[] command = "SET k v".getBytes(StandardCharsets.UTF_8);
        durable.apply(1L, command);
        durable.apply(2L, command);
        durable.apply(1L, command);   // the replay after a restart
        durable.apply(2L, command);

        assertEquals(2, durable.applications);
    }

    private String read(String query) {
        return new String(sm.read(query.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
    }

    @Test
    void restoreWithCorruptDataThrows() {
        assertThrows(IllegalArgumentException.class,
                () -> sm.restoreSnapshot(new byte[]{0, 0, 0, 10, 'x'})); // count=10 but no data
    }
}
