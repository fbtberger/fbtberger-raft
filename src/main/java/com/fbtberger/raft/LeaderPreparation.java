/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

/**
 * Implemented, in addition to {@link StateMachine}, by a state machine that needs
 * one look at a command <em>on the leader</em> before the command is appended to
 * the log. What {@link #prepare} returns is what gets appended, replicated and
 * later handed to {@link StateMachine#apply(long, byte[])} on every server --
 * including the leader itself.
 *
 * <p>This is deliberately not part of {@link StateMachine}. Almost no state
 * machine needs it, and one that does not should not carry a method that invites
 * it to put a clock or a random number into its commands. {@code RaftNode} calls
 * {@link #prepare} only for commands addressed to a state machine that implements
 * this interface.
 *
 * <p>This is the only place such a state machine may put something
 * non-deterministic into a command. {@code apply} runs on every server and must
 * compute the same thing everywhere, so it cannot read a clock or draw a random
 * number. The leader can: whatever it writes here is fixed in the log entry, and
 * the followers replicate the value rather than recompute it.
 *
 * <p>Obligations on implementations:
 * <ol>
 *   <li><b>Be fast and do no I/O.</b> {@code RaftNode} calls this under the Raft
 *       lock, between the leadership check and the append.</li>
 *   <li><b>Do not change state.</b> The command is not committed yet and may
 *       never be: a leader that loses its term discards uncommitted entries.
 *       State changes belong in {@code apply}.</li>
 *   <li><b>Do not rely on being called.</b> It runs once per client submission
 *       on the server that was leader at that moment, and never on replay. A
 *       command that needed preparing and did not get it must be recognisable
 *       as such in {@code apply}.</li>
 * </ol>
 *
 * <p>An exception thrown here fails the client's request and appends nothing, and
 * so does a {@code null} result.
 */
public interface LeaderPreparation {

    /** Returns the command as it is to be appended to the log. */
    byte[] prepare(byte[] command);
}
