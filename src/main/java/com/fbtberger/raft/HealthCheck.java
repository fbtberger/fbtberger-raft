/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
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

    /**
     * Readiness = "can this node make progress?", and is therefore <em>quorum-aware</em>
     * (see {@link RaftNode#isReadyToServe()}). It reports DOWN not only when no leader is known,
     * but also for a leader that has lost its quorum and for a follower whose remembered leader
     * has gone unreachable — both of which would otherwise still expose a stale {@code role} /
     * {@code currentLeaderId} while being unable to serve.
     */
    public Status readiness() {
        return readinessStatus(node.role(), node.isReadyToServe(), node.currentLeaderId());
    }

    /**
     * Pure readiness-to-{@link Status} mapping. {@code readyToServe} decides UP/DOWN; role and
     * leader id only shape the human-readable message.
     */
    static Status readinessStatus(ServerRole role, boolean readyToServe, String leaderId) {
        if (readyToServe) {
            return role == ServerRole.LEADER
                    ? new Status(true, "leader")
                    : new Status(true, "follower, leader=" + leaderId);
        }
        return new Status(false, role == ServerRole.LEADER
                ? "leader without quorum"
                : "no recent leader contact");
    }

    public record Status(boolean ok, String message) {
        public String toJson() {
            return "{\"status\":\"" + (ok ? "UP" : "DOWN") + "\",\"message\":\"" + message + "\"}";
        }
    }
}
