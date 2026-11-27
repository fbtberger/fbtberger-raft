/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * The state machine of a witness: none. Every command is accepted and forgotten, a
 * snapshot is empty, and a snapshot handed in is ignored.
 *
 * <p>{@link RaftNode} puts this in place of whatever state machine it was given when it
 * runs as a witness, so the rest of the node -- applying committed entries, restoring at
 * start -- needs no second code path.
 */
final class WitnessStateMachine implements StateMachine {

    private static final byte[] NOTHING = new byte[0];

    @Override
    public byte[] apply(byte[] command) {
        return NOTHING;
    }

    @Override
    public byte[] takeSnapshot() {
        return NOTHING;
    }

    @Override
    public void restoreSnapshot(byte[] snapshot) {
        // Nothing to restore into.
    }

    @Override
    public void writeSnapshot(OutputStream out) {
        // Nothing to write.
    }

    @Override
    public SnapshotWriter prepareSnapshotWriter() {
        return out -> { };
    }

    @Override
    public void restoreSnapshot(InputStream in) {
        // Not read: a witness given a data node's directory leaves the payload alone.
    }
}
