/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * The copy-on-write snapshot promise (§5.1, §7) that <b>every</b> {@link StateMachine}
 * implementation must keep — run against every implementation.
 *
 * <h2>The promise</h2>
 * {@link StateMachine#prepareCowSnapshot()} is called by {@link RaftNode} <em>under the Raft
 * lock</em>, together with the index the snapshot will be labelled with:
 *
 * <pre>
 *   long applied = lastApplied.get();                    // the label
 *   Supplier&lt;byte[]&gt; cow = stateMachine.prepareCowSnapshot();   // the state
 * </pre>
 *
 * The supplier is then invoked much later, on a background thread, while {@code apply()} keeps
 * running. So the supplier must serialize the state <b>as it was at the moment of capture</b>. If
 * it instead serializes whatever the state machine looks like when the supplier finally runs, the
 * snapshot is labelled {@code lastIncludedIndex = N} but contains the effects of N+1, N+2, … — and
 * {@code saveSnapshotAndCompact} then discards the log up to N.
 *
 * <p><b>What that costs.</b> On restart, entry N+1 is replayed on top of a state that already
 * contains it. For a key-value store with idempotent {@code SET}s, nothing visible happens — which
 * is exactly why such a bug can live for years. For a state machine whose commands are <em>not</em>
 * idempotent (a counter, a game applying a move, anything that consumes from a hand), it is
 * silent corruption.
 *
 * <h2>The trap</h2>
 * This one-liner compiles, reads correctly, and is fast:
 *
 * <pre>
 *   public Supplier&lt;byte[]&gt; prepareCowSnapshot() { return this::takeSnapshot; }
 * </pre>
 *
 * It is also completely wrong: it defers the serialization, so it sees every {@code apply()} that
 * happens in between. A shallow copy of a map is not enough either, if the map's <b>values are
 * mutable</b> — the copy shares them, and the background serializer watches them change.
 *
 * <p>{@link BrokenLazyStateMachine} below is exactly that mistake, and it exists so this suite can
 * demonstrate that it <em>catches</em> it. A contract test that cannot fail is decoration.
 */
abstract class StateMachineCowContract {

    /** A fresh, empty state machine of the implementation under test. */
    protected abstract StateMachine create();

    /** A command that sets {@code key} to {@code value} in this implementation's language. */
    protected abstract byte[] setCommand(String key, String value);

    /** The value {@code key} has in a state machine restored from {@code snapshot}. */
    protected abstract String valueAfterRestore(byte[] snapshot, String key);

    @Test
    void aSnapshotContainsWhatWasThereWhenItWasCaptured() {
        StateMachine sm = create();
        sm.apply(setCommand("a", "1"));

        Supplier<byte[]> cow = sm.prepareCowSnapshot();

        assertEquals("1", valueAfterRestore(cow.get(), "a"));
    }

    /**
     * THE INVARIANT. Everything applied after the capture must be invisible to it — that is the
     * whole reason the capture happens under the Raft lock while the serialization does not.
     */
    @Test
    void applyingAfterTheCaptureDoesNotChangeTheSnapshot() {
        StateMachine sm = create();
        sm.apply(setCommand("a", "before"));

        Supplier<byte[]> cow = sm.prepareCowSnapshot();   // labelled with the index reached here

        sm.apply(setCommand("a", "after"));               // index + 1 — must NOT be in the snapshot
        sm.apply(setCommand("b", "also-after"));          // index + 2 — nor this

        byte[] snapshot = cow.get();                      // serialized later, on another thread

        assertEquals("before", valueAfterRestore(snapshot, "a"),
                "the snapshot serialized state from AFTER its own lastIncludedIndex — "
                        + "on restore, those entries would be applied a second time");
        assertEquals(null, valueAfterRestore(snapshot, "b"),
                "a key created after the capture must not appear in it");
    }

    @Test
    void theStateMachineItselfKeepsMovingAfterACapture() {
        // The capture must not freeze or roll back the live state — only the snapshot is frozen.
        StateMachine sm = create();
        sm.apply(setCommand("a", "before"));

        Supplier<byte[]> cow = sm.prepareCowSnapshot();
        sm.apply(setCommand("a", "after"));

        byte[] live = sm.takeSnapshot();
        assertEquals("after", valueAfterRestore(live, "a"));
        assertNotEquals("after", valueAfterRestore(cow.get(), "a"));
    }

    @Test
    void twoCapturesAreIndependent() {
        StateMachine sm = create();
        sm.apply(setCommand("a", "1"));
        Supplier<byte[]> first = sm.prepareCowSnapshot();

        sm.apply(setCommand("a", "2"));
        Supplier<byte[]> second = sm.prepareCowSnapshot();

        sm.apply(setCommand("a", "3"));

        assertEquals("1", valueAfterRestore(first.get(), "a"));
        assertEquals("2", valueAfterRestore(second.get(), "a"));
    }

    @Test
    void anEmptyStateMachineSnapshotsCleanly() {
        StateMachine sm = create();
        Supplier<byte[]> cow = sm.prepareCowSnapshot();
        sm.apply(setCommand("a", "1"));

        assertEquals(null, valueAfterRestore(cow.get(), "a"));
    }

    // ── the negative control ─────────────────────────────────────────────────

    /**
     * A state machine that makes the mistake this contract exists to catch: it hands back a
     * supplier that serializes <em>lazily</em>, so it sees every {@code apply()} that happens
     * between the capture and the serialization.
     *
     * <p>It is here so that {@link BrokenLazyStateMachineTest} can assert the suite <b>fails</b>
     * against it. Without that, a green contract suite proves nothing about the contract — only
     * that the implementations happen to agree with it.
     */
    static final class BrokenLazyStateMachine implements StateMachine {

        private final java.util.Map<String, String> data = new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public byte[] apply(byte[] command) {
            String cmd = new String(command, StandardCharsets.UTF_8);
            String[] parts = cmd.split(" ", 3);
            if (parts.length == 3 && parts[0].equals("SET")) data.put(parts[1], parts[2]);
            return new byte[0];
        }

        @Override
        public byte[] takeSnapshot() {
            StringBuilder sb = new StringBuilder();
            data.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
            return sb.toString().getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public void restoreSnapshot(byte[] snapshot) {
            data.clear();
            String text = new String(snapshot, StandardCharsets.UTF_8);
            for (String line : text.split("\n")) {
                if (line.isBlank()) continue;
                int eq = line.indexOf('=');
                data.put(line.substring(0, eq), line.substring(eq + 1));
            }
        }

        /** The bug: serialization is deferred, so it observes state from AFTER the capture. */
        @Override
        public Supplier<byte[]> prepareCowSnapshot() {
            return this::takeSnapshot;
        }

        String get(String key) {
            return data.get(key);
        }
    }
}
