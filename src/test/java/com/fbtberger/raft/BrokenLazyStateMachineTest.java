/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * The negative control: proof that {@link StateMachineCowContract} can actually FAIL.
 *
 * <p>A contract suite that only ever runs against implementations which happen to satisfy it tells
 * you nothing about the contract — it tells you the implementations agree with each other. This
 * test takes the mistake the contract exists to catch and shows the contract catching it.
 *
 * <p>The mistake is a one-liner, and it is the one a hurried implementer will write:
 *
 * <pre>
 *   public Supplier&lt;byte[]&gt; prepareCowSnapshot() { return this::takeSnapshot; }
 * </pre>
 *
 * Fast, correct-looking, and it silently defers serialization until the background thread runs —
 * by which time {@code apply()} has moved the state machine past the index the snapshot claims to
 * be. On restore, those entries get applied a second time.
 */
class BrokenLazyStateMachineTest {

    @Test
    void aLazySupplierSeesStateFromAfterTheCapture_andTheContractWouldCatchIt() {
        var sm = new StateMachineCowContract.BrokenLazyStateMachine();
        sm.apply(set("a", "before"));

        Supplier<byte[]> cow = sm.prepareCowSnapshot();   // "captured" at this index...
        sm.apply(set("a", "after"));                      // ...but this lands in it anyway

        var restored = new StateMachineCowContract.BrokenLazyStateMachine();
        restored.restoreSnapshot(cow.get());

        // This is the bug, asserted as a fact: the snapshot contains state the snapshot's own
        // lastIncludedIndex says it cannot contain.
        assertEquals("after", restored.get("a"),
                "the broken machine leaked post-capture state — which is the point of this test");
        assertNotEquals("before", restored.get("a"));
    }

    @Test
    void theSameSequenceAgainstAConformingMachineKeepsThePromise() {
        // Side by side, so the difference is impossible to miss.
        var sm = new KeyValueStateMachine();
        sm.apply(set("a", "before"));

        Supplier<byte[]> cow = sm.prepareCowSnapshot();
        sm.apply(set("a", "after"));

        var restored = new KeyValueStateMachine();
        restored.restoreSnapshot(cow.get());

        assertEquals("before", restored.get("a"));
    }

    private static byte[] set(String key, String value) {
        return ("SET " + key + " " + value).getBytes(StandardCharsets.UTF_8);
    }
}
