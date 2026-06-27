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
