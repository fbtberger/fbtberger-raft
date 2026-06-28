/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

public final class HealthCheck {

    private final RaftNode node;
    private final RaftStorage store;

    public HealthCheck(RaftNode node, RaftStorage store) {
        this.node = node;
        this.store = store;
    }

    public Status liveness() {
        return new Status(true, "alive");
    }

    public Status readiness() {
        if (node.role() == ServerRole.LEADER) {
            return new Status(true, "leader");
        }
        if (node.currentLeaderId() != null) {
            return new Status(true, "follower, leader=" + node.currentLeaderId());
        }
        return new Status(false, "no leader known");
    }

    public record Status(boolean ok, String message) {
        public String toJson() {
            return "{\"status\":\"" + (ok ? "UP" : "DOWN") + "\",\"message\":\"" + message + "\"}";
        }
    }
}
