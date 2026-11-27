/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.util.Objects;

/**
 * Registers one additional state machine with a {@link RaftNode} under a name.
 *
 * <p>A node always has its application state machine. Every bean of this type
 * adds one more, with a domain of its own: its own commands, its own state and
 * its own section in the snapshot. Raft delivers a log entry to exactly one of
 * them, chosen by the name the entry carries (see {@link StateMachineRegistry}).
 *
 * <p>The state machine is wrapped in this record on purpose rather than exposed
 * as a {@link StateMachine} bean: there stays exactly one {@code StateMachine}
 * bean in the context, the application's.
 *
 * @param name         how commands address this state machine; see
 *                     {@link StateMachineRegistry#validateName}
 * @param stateMachine the state machine itself
 */
public record StateMachineRegistration(String name, StateMachine stateMachine) {

    public StateMachineRegistration {
        StateMachineRegistry.validateName(name);
        Objects.requireNonNull(stateMachine, "stateMachine");
    }
}
