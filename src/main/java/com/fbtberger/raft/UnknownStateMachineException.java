/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

/** A command, a query or a snapshot section names a state machine this node does not have. */
public class UnknownStateMachineException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    private final String stateMachine;

    public UnknownStateMachineException(String stateMachine, String message) {
        super(message);
        this.stateMachine = stateMachine;
    }

    /** The name that was asked for. */
    public String stateMachine() {
        return stateMachine;
    }
}
