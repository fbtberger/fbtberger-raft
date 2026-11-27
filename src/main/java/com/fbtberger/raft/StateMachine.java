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
     * Applies one committed command, told which log index it is.
     *
     * <p>Only a state machine whose state <em>outlives the process</em> needs this.
     * An in-memory one starts empty on every restart, so replaying the log from the
     * beginning reconstructs exactly the right state and the index is redundant. A
     * state machine backed by a database does not start empty: on restart its data
     * already reflects everything it applied before, and Raft -- which has no
     * snapshot yet, or one older than that data -- replays those same entries
     * again.
     *
     * <p>Measured, because this is not theoretical. A three-node cluster with a
     * SQL-backed state machine, one node killed mid-load and restarted: it caught
     * up to the same row count as its peers and held different data. Row versions
     * had been incremented a second time and at least one value ended up different,
     * because every command between its last commit and the kill was applied twice.
     * Same key count, different content, no error anywhere.
     *
     * <p>An implementation with durable storage must therefore record the index it
     * has applied <b>in the same transaction as the data</b>, and ignore any index
     * it has already seen. The default here simply delegates, which is correct for
     * every in-memory implementation and keeps this an additive change.
     */
    default byte[] apply(long index, byte[] command) {
        return apply(command);
    }

    /**
     * Gives the state machine one look at a client command <em>on the leader</em>,
     * before the command is appended to the log. What this returns is what gets
     * appended, replicated and later handed to {@link #apply(long, byte[])} on every
     * server -- including this one.
     *
     * <p>This is the only place a state machine may put something non-deterministic
     * into a command. {@link #apply} runs on every server and must compute the same
     * thing everywhere, so it cannot read a clock or draw a random number. The
     * leader can: whatever it writes here is fixed in the log entry, and the
     * followers replicate the value rather than recompute it.
     *
     * <p>Obligations on implementations:
     * <ol>
     *   <li><b>Be fast and do no I/O.</b> {@code RaftNode} calls this under the Raft
     *       lock, between the leadership check and the append.</li>
     *   <li><b>Do not change state.</b> The command is not committed yet and may
     *       never be: a leader that loses its term discards uncommitted entries.
     *       State changes belong in {@link #apply}.</li>
     *   <li><b>Do not rely on being called.</b> It runs once per client submission
     *       on the server that was leader at that moment, and never on replay. A
     *       command that needed preparing and did not get it must be recognisable
     *       as such in {@link #apply}.</li>
     * </ol>
     *
     * <p>An exception thrown here fails the client's request and appends nothing.
     * The default returns the command unchanged, which keeps this an additive
     * change for every existing implementation.
     */
    default byte[] prepare(byte[] command) {
        return command;
    }

    /**
     * Answers a read-only query against the current state, without going through
     * the log.
     *
     * <p>Reads are deliberately not commands: replicating a query would put an
     * entry in every server's log and cost a round of consensus to learn something
     * nobody changed. Linearizability comes from the caller instead --
     * {@link RaftNode#query} confirms leadership with a ReadIndex barrier (§6.4)
     * and waits until everything committed before the query has been applied,
     * <em>then</em> calls this method.
     *
     * <p>Two obligations on implementations:
     * <ol>
     *   <li><b>Do not mutate.</b> This runs outside the log, so anything changed
     *       here exists on one server only -- divergence that no snapshot or
     *       replay can repair.</li>
     *   <li><b>Be safe against a concurrent {@link #apply}.</b> Unlike apply,
     *       which Raft calls single-threaded, this can run on a client thread
     *       while the applier thread is working.</li>
     * </ol>
     *
     * <p>The default refuses: a state machine that has no read side should say so
     * rather than silently answer something empty.
     */
    default byte[] read(byte[] query) {
        throw new UnsupportedOperationException(
                getClass().getSimpleName() + " serves no reads");
    }

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
