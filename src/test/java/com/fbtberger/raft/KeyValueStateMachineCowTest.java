/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.nio.charset.StandardCharsets;

/**
 * {@link StateMachineCowContract} against {@link KeyValueStateMachine}.
 *
 * <p>It keeps the promise for a reason worth naming: its {@code prepareCowSnapshot()} copies the
 * map <em>and</em> its values are {@link String}s — immutable. A shallow copy is only enough
 * because nothing in it can change afterwards. A state machine holding mutable objects would need
 * a deeper copy, or serialization under the lock.
 */
class KeyValueStateMachineCowTest extends StateMachineCowContract {

    @Override
    protected StateMachine create() {
        return new KeyValueStateMachine();
    }

    @Override
    protected byte[] setCommand(String key, String value) {
        return ("SET " + key + " " + value).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    protected String valueAfterRestore(byte[] snapshot, String key) {
        KeyValueStateMachine restored = new KeyValueStateMachine();
        restored.restoreSnapshot(snapshot);
        return restored.get(key);
    }
}
