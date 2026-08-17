/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.transport.RpcTimeouts;
import com.fbtberger.raft.transport.TlsConfig;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the programmatic {@link RaftConfig#of} factory used by callers that wire their config in
 * code rather than from a {@code .properties} file.
 */
class RaftConfigOfTest {

    private static final Map<String, String> THREE_NODES = Map.of(
            "n1", "localhost:9091",
            "n2", "localhost:9092",
            "n3", "localhost:9093");

    @Test
    void of_withDefaults_populatesCoreFieldsAndClusterMath() {
        RaftConfig cfg = RaftConfig.of("n2", 9092, Path.of("/var/raft/n2"), THREE_NODES);

        assertEquals("n2", cfg.selfId());
        assertEquals(9092, cfg.selfPort());
        assertEquals(Path.of("/var/raft/n2"), cfg.dataDir());
        assertEquals(3, cfg.clusterSize());
        assertEquals(2, cfg.majority());
        assertEquals(0, cfg.metricsPort());
        assertFalse(cfg.isLearner());
        assertFalse(cfg.tlsConfig().enabled());
    }

    @Test
    void of_full_passesThroughSettings() {
        RpcTimeouts timeouts = RpcTimeouts.defaults();
        TlsConfig tls = TlsConfig.disabled();

        RaftConfig cfg = RaftConfig.of("n1", 9091, Path.of("/tmp/n1"), THREE_NODES,
                Integer.MAX_VALUE, 65_536, 9101, true, timeouts, tls);

        assertEquals(Integer.MAX_VALUE, cfg.snapshotThreshold());
        assertEquals(65_536, cfg.snapshotChunkSize());
        assertEquals(9101, cfg.metricsPort());
        assertTrue(cfg.isLearner());
        assertSame(timeouts, cfg.rpcTimeouts());
        assertSame(tls, cfg.tlsConfig());
    }

    @Test
    void of_requiresSelfInPeerList() {
        assertThrows(IllegalArgumentException.class,
                () -> RaftConfig.of("nX", 9099, Path.of("/tmp/nX"), THREE_NODES));
    }

    @Test
    void of_singleNode_hasMajorityOfOne() {
        RaftConfig cfg = RaftConfig.of("solo", 9090, Path.of("/tmp/solo"),
                Map.of("solo", "localhost:9090"));
        assertEquals(1, cfg.clusterSize());
        assertEquals(1, cfg.majority());
    }
}
