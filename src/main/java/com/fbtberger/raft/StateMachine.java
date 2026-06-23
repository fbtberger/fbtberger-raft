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
     * Replaces this state machine's entire current state with what's encoded
     * in {@code snapshot}, as produced by a prior call to
     * {@link #takeSnapshot}. Called once, before any further {@link #apply}
     * calls, whenever this server restores its own snapshot at startup or
     * installs one received from the leader (§7).
     */
    void restoreSnapshot(byte[] snapshot);
}
