/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class HealthCheckTest {

    @Test
    void livenessAlwaysReturnsUp() {
        RaftNode node = createFollower();
        try {
            HealthCheck hc = new HealthCheck(node, new InMemoryStorage());
            HealthCheck.Status status = hc.liveness();
            assertTrue(status.ok());
            assertTrue(status.toJson().contains("UP"));
        } finally { node.shutdown(); }
    }

    @Test
    void readinessIsDownWhenNoLeaderKnown() {
        RaftNode node = createFollower();
        try {
            HealthCheck hc = new HealthCheck(node, new InMemoryStorage());
            HealthCheck.Status status = hc.readiness();
            assertFalse(status.ok());
            assertTrue(status.toJson().contains("DOWN"));
        } finally { node.shutdown(); }
    }

    @Test
    void readinessIsUpWhenNodeIsLeader() throws Exception {
        RaftConfig cfg = singleNodeConfig();
        RaftNode node = new RaftNode(cfg, new InMemoryStorage(),
                new KeyValueStateMachine(), addr -> null, RaftMetrics.noop());
        try {
            node.start();
            Thread.sleep(200);
            HealthCheck hc = new HealthCheck(node, new InMemoryStorage());
            HealthCheck.Status status = hc.readiness();
            assertTrue(status.ok());
            assertTrue(status.toJson().contains("leader"));
        } finally { node.shutdown(); }
    }

    // ── quorum-aware readiness mapping (pure) ─────────────────────────────────

    @Test
    void readinessStatusLeaderWithQuorumIsUp() {
        HealthCheck.Status s = HealthCheck.readinessStatus(ServerRole.LEADER, false, true, null);
        assertTrue(s.ok());
        assertEquals("leader", s.message());
    }

    @Test
    void readinessStatusFollowerWithFreshLeaderIsUp() {
        HealthCheck.Status s = HealthCheck.readinessStatus(ServerRole.FOLLOWER, false, true, "n2");
        assertTrue(s.ok());
        assertTrue(s.message().contains("leader=n2"));
    }

    @Test
    void readinessStatusLeaderWithoutQuorumIsDown() {
        HealthCheck.Status s = HealthCheck.readinessStatus(ServerRole.LEADER, false, false, null);
        assertFalse(s.ok());
        assertEquals("leader without quorum", s.message());
    }

    @Test
    void readinessStatusFollowerWithStaleLeaderIsDown() {
        HealthCheck.Status s = HealthCheck.readinessStatus(ServerRole.FOLLOWER, false, false, "n3");
        assertFalse(s.ok());
        assertTrue(s.message().contains("no recent leader contact"));
    }

    // ── learners are reported as such (v107) ──────────────────────────────────
    //
    // Every non-leader used to report "follower", so a learner and a voting follower looked
    // identical from outside. They are not the same thing when one of them disappears: a missing
    // voter eats into the quorum, a missing learner only costs read capacity. During the July
    // outage, "which of the empty nodes are voters?" was exactly the question the health endpoint
    // could not answer.

    @Test
    void aHealthyLearnerSaysLearner_notFollower() {
        HealthCheck.Status s = HealthCheck.readinessStatus(ServerRole.FOLLOWER, true, true, "n2");
        assertTrue(s.ok());
        assertEquals("learner, leader=n2", s.message());
    }

    @Test
    void aVotingFollowerStillSaysFollower() {
        HealthCheck.Status s = HealthCheck.readinessStatus(ServerRole.FOLLOWER, false, true, "n2");
        assertEquals("follower, leader=n2", s.message());
    }

    @Test
    void aLearnerThatLostTheLeaderSaysSo_butIsStillIdentifiableAsALearner() {
        HealthCheck.Status s = HealthCheck.readinessStatus(ServerRole.FOLLOWER, true, false, "n3");
        assertFalse(s.ok());
        assertTrue(s.message().startsWith("learner"), s.message());
        assertTrue(s.message().contains("no recent leader contact"), s.message());
    }

    @Test
    void aLearnerIsNotAFourthRole() {
        // §4.2.1: a learner is a non-voting MEMBER, and its role is FOLLOWER like any other.
        // Membership and role are different questions; ServerRole stays exactly as Figure 4
        // has it, and the learner flag rides alongside rather than inside it.
        assertEquals(3, ServerRole.values().length);
    }

    // ── isReadyToServe integration (achievable node states) ───────────────────

    @Test
    void isReadyToServeIsFalseForAFreshFollowerWithNoLeaderContact() {
        RaftNode node = createFollower();
        try {
            assertFalse(node.isReadyToServe());
        } finally { node.shutdown(); }
    }

    @Test
    void isReadyToServeIsTrueForASingleNodeLeader() throws Exception {
        RaftConfig cfg = singleNodeConfig();
        RaftNode node = new RaftNode(cfg, new InMemoryStorage(),
                new KeyValueStateMachine(), addr -> null, RaftMetrics.noop());
        try {
            node.start();
            Thread.sleep(200);
            assertTrue(node.isReadyToServe());
        } finally { node.shutdown(); }
    }

    private static RaftNode createFollower() {
        try {
            return new RaftNode(multiNodeConfig(), new InMemoryStorage(),
                    new KeyValueStateMachine(), addr -> null, RaftMetrics.noop());
        } catch (Exception e) { throw new RuntimeException(e); }
    }

    private static RaftConfig singleNodeConfig() throws Exception {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-test-unused");
        props.setProperty("peer.n1", "localhost:9091");
        props.setProperty("snapshot.threshold", "100");
        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-hc-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) { props.store(out, null); }
        return RaftConfig.load(tmp);
    }

    private static RaftConfig multiNodeConfig() throws Exception {
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-test-unused");
        props.setProperty("peer.n1", "localhost:9091");
        props.setProperty("peer.n2", "localhost:9092");
        props.setProperty("peer.n3", "localhost:9093");
        props.setProperty("snapshot.threshold", "100");
        java.nio.file.Path tmp = java.nio.file.Files.createTempFile("raft-hc-", ".properties");
        try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(tmp)) { props.store(out, null); }
        return RaftConfig.load(tmp);
    }
}
