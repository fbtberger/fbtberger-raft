/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

/**
 * The replicated state machine that Raft drives (paper, Section 2): every
 * server applies exactly the same sequence of committed commands, in the
 * same order, so they all end up in the same state.
 */
public interface StateMachine {

    /**
     * Applies one committed command and returns whatever result should be
     * handed back to the client that submitted it.
     */
    byte[] apply(byte[] command);

    /**
     * Produces a complete, self-contained snapshot of this state machine's
     * current state (§7), to be persisted by {@link RaftNode} alongside the
     * Raft-level snapshot metadata (lastIncludedIndex/Term) and handed back
     * verbatim to {@link #restoreSnapshot} later -- on this server after a
     * restart, or on a follower installing a snapshot it received from the
     * leader. The encoding is entirely up to the implementation; Raft treats
     * the result as an opaque byte string.
     */
    byte[] takeSnapshot();

    /**
     * Copy-on-write snapshot capture: returns a supplier that, when called
     * later (possibly on a different thread, without the Raft lock), produces
     * the serialized snapshot bytes. The method itself must be fast (O(1) or
     * shallow-copy) because it runs under the Raft lock; the expensive
     * serialization is deferred to the supplier.
     *
     * <p>The default implementation eagerly serializes via {@link #takeSnapshot()},
     * which is correct but blocks the lock for the duration of serialization.
     * Implementations with large state should override this to capture a
     * lightweight copy-on-write reference and serialize lazily.
     *
     * <h2>The contract — and it is a safety contract, not a performance one</h2>
     * {@code RaftNode} calls this under the Raft lock, in the same breath as reading the index the
     * snapshot will be labelled with:
     *
     * <pre>
     *   long applied = lastApplied.get();                 // the label
     *   Supplier&lt;byte[]&gt; cow = prepareCowSnapshot();      // the state
     * </pre>
     *
     * <b>The supplier must serialize the state as it was at the moment of capture.</b> Everything
     * {@link #apply} does afterwards must be invisible to it. If post-capture state leaks in, the
     * snapshot is labelled {@code lastIncludedIndex = N} while containing the effects of N+1 — and
     * the log up to N is then discarded. On restart, N+1 is applied a <b>second</b> time. With
     * idempotent commands nothing visible happens, which is how such a bug survives for years; with
     * commands that consume state (a counter, a move that takes cards from a hand) it is silent
     * corruption.
     *
     * <h2>Two ways to get this wrong</h2>
     * <ol>
     *   <li><b>Deferring the read instead of the serialization.</b> This compiles, reads correctly,
     *       and is wrong:
     *       <pre>  return this::takeSnapshot;  // serializes LATER — sees every apply() in between</pre>
     *   </li>
     *   <li><b>A shallow copy of mutable values.</b> {@code new HashMap&lt;&gt;(data)} isolates the
     *       map, not what is in it. If the values are mutable objects, the copy shares them and the
     *       background serializer watches them change. It is safe in
     *       {@code KeyValueStateMachine} only because its values are {@link String}s.</li>
     * </ol>
     *
     * <p>{@code StateMachineCowContract} in the test sources states this invariant as executable
     * assertions; run any new implementation against it.
     */
    default java.util.function.Supplier<byte[]> prepareCowSnapshot() {
        byte[] data = takeSnapshot();
        return () -> data;
    }

    /**
     * Replaces this state machine's entire current state with what's encoded
     * in {@code snapshot}, as produced by a prior call to
     * {@link #takeSnapshot}. Called once, before any further {@link #apply}
     * calls, whenever this server restores its own snapshot at startup or
     * installs one received from the leader (§7).
     */
    void restoreSnapshot(byte[] snapshot);
}
