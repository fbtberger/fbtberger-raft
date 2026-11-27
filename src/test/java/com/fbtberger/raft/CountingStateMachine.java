/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.nio.ByteBuffer;

/**
 * Adds one per applied command and answers with the new total. The smallest state machine
 * whose commands are not idempotent, which is the property a de-duplication test needs: a
 * command applied twice leaves a different state behind than one applied once.
 */
final class CountingStateMachine implements StateMachine {

    private long value;

    @Override
    public synchronized byte[] apply(byte[] command) {
        if (command.length == 0) {
            return new byte[0]; // the no-op a new leader commits
        }
        value++;
        return encode(value);
    }

    synchronized long value() {
        return value;
    }

    @Override
    public synchronized byte[] takeSnapshot() {
        return encode(value);
    }

    @Override
    public synchronized void restoreSnapshot(byte[] snapshot) {
        value = ByteBuffer.wrap(snapshot).getLong();
    }

    static byte[] encode(long v) {
        return ByteBuffer.allocate(Long.BYTES).putLong(v).array();
    }
}
