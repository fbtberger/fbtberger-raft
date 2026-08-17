/*
 * Copyright 2026 fbtBerger Technology
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
        return readinessStatus(node.role(), node.isLearner(),
                node.isReadyToServe(), node.currentLeaderId());
    }

    /**
     * Stricter than {@link #readiness()}: may this node <b>serve reads</b>? (v105)
     *
     * <p>Readiness alone is not enough, as an outage on the dev cluster showed: three nodes had an
     * empty state machine — they applied nothing for hours — while still reporting UP, because
     * "leader contact" only meant that bytes had arrived. This variant additionally requires that
     * everything the leader has declared committed has actually been APPLIED here
     * ({@link RaftNode#isCaughtUp()}). A node that is behind must say so, so that traffic goes
     * elsewhere rather than being told the data does not exist.
     */
    public Status serving() {
        boolean learner = node.isLearner();
        if (!node.isReadyToServe()) {
            return readinessStatus(node.role(), learner, false, node.currentLeaderId());
        }
        if (!node.isCaughtUp()) {
            return new Status(false, (learner ? "learner " : "") + "behind: applied="
                    + node.appliedIndex() + " leaderCommit=" + node.leaderCommitSeen());
        }
        return readinessStatus(node.role(), learner, true, node.currentLeaderId());
    }

    /**
     * Pure readiness-to-{@link Status} mapping. {@code readyToServe} decides UP/DOWN; the role,
     * the learner flag and the leader id only shape the human-readable message.
     *
     * <p><b>Why the learner flag is not part of {@link ServerRole}</b> (v107). A learner is not a
     * fourth role: §4.2.1 makes it a non-voting <em>member</em>, and it is a FOLLOWER like any
     * other. Membership and role are different questions, and folding one into the other would
     * misrepresent the algorithm — so the role enum stays exactly as Figure 4 has it, and the
     * flag rides alongside.
     *
     * <p><b>Why it is reported at all.</b> The message used to say {@code follower} for every
     * non-leader, so a learner and a voting follower were indistinguishable from the outside. But
     * losing one of them is not the same event: <b>a missing voter eats into the quorum; a missing
     * learner only costs read capacity.</b> In the July outage three of five nodes sat with an
     * empty state machine, and which of them were voters was precisely the question that mattered
     * — and precisely the one the health endpoint could not answer.
     */
    static Status readinessStatus(ServerRole role, boolean learner,
                                  boolean readyToServe, String leaderId) {
        if (readyToServe) {
            if (role == ServerRole.LEADER) return new Status(true, "leader");
            return new Status(true, (learner ? "learner" : "follower") + ", leader=" + leaderId);
        }
        if (role == ServerRole.LEADER) return new Status(false, "leader without quorum");
        return new Status(false, (learner ? "learner, " : "") + "no recent leader contact");
    }

    public record Status(boolean ok, String message) {
        public String toJson() {
            return "{\"status\":\"" + (ok ? "UP" : "DOWN") + "\",\"message\":\"" + message + "\"}";
        }
    }
}
