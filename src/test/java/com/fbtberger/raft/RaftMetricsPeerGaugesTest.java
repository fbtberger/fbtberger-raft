/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The per-peer replication gauges, whose reason for existing is that the set of series
 * on a leader is the live configuration.
 *
 * <p>Everything asserted here is about a series appearing or disappearing rather than
 * about a number, because the number was already available in the log line these gauges
 * were lifted from. What was not available was membership, and membership is expressed
 * by which series exist.
 */
class RaftMetricsPeerGaugesTest {

    private SimpleMeterRegistry registry;
    private RaftMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new RaftMetrics(registry, "node1");
    }

    @Test
    void theSeriesOnALeaderAreItsConfiguration() {
        metrics.peerReplication("node2", "voter", 100, 40);
        metrics.peerReplication("node3", "voter", 98, 45);
        metrics.peerReplication("node4", "learner", 97, 50);

        assertEquals(Set.of("node2", "node3", "node4"), peersWithSeries());
        assertEquals("learner", roleOf("node4"));
        assertEquals(100.0, matchIndexOf("node2"));
    }

    @Test
    void promotingALearnerDoesNotPublishItTwice() {
        metrics.peerReplication("node4", "learner", 90, 30);
        metrics.peerReplication("node4", "voter", 95, 30);

        // Micrometer keys a meter by name plus tags, so the promoted node would otherwise
        // appear under both roles at once -- and a reader counting voters would get four
        // where there are three. Which is exactly the class of quiet wrongness these
        // gauges were added to remove.
        List<Gauge> forNode4 = registry.find("raft.replication.match.index")
                .tag("peer", "node4").gauges().stream().toList();

        assertEquals(1, forNode4.size(), "node4 is published once, under one role");
        assertEquals("voter", roleOf("node4"));
        assertEquals(95.0, matchIndexOf("node4"));
    }

    @Test
    void aRemovedPeerStopsBeingPublished() {
        metrics.peerReplication("node2", "voter", 100, 40);
        metrics.peerReplication("node3", "voter", 100, 40);

        metrics.forgetPeersExcept(List.of("node2"));

        // A series claiming the leader still replicates to a node it has removed is worse
        // than no series: raft.cluster.size already has that failure mode -- a removed
        // node keeps reporting the last size it knew -- and these gauges exist to be the
        // thing that does not lie about membership.
        assertEquals(Set.of("node2"), peersWithSeries());
    }

    @Test
    void aNodeThatStepsDownPublishesNothing() {
        metrics.peerReplication("node2", "voter", 100, 40);
        metrics.peerReplication("node3", "learner", 100, 40);

        metrics.forgetPeersExcept(List.of());

        assertTrue(peersWithSeries().isEmpty(),
                "only a leader knows any of this; a follower claiming to would be a lie");
    }

    @Test
    void aPeerThatHasNeverAcknowledgedIsNotReportedAsRecent() {
        metrics.peerReplication("node2", "voter", 0, -1);

        double seconds = registry.find("raft.replication.last.ack.seconds")
                .tag("peer", "node2").gauge().value();

        // Zero would read as "acknowledged just now", which is the opposite of the truth
        // and the more dangerous direction to be wrong in.
        assertTrue(Double.isNaN(seconds), "never acknowledged must not look like just now");
    }

    private Set<String> peersWithSeries() {
        return registry.find("raft.replication.match.index").gauges().stream()
                .map(g -> g.getId().getTag("peer"))
                .collect(Collectors.toSet());
    }

    private String roleOf(String peer) {
        return registry.find("raft.replication.match.index").tag("peer", peer)
                .gauge().getId().getTag("peer_role");
    }

    private double matchIndexOf(String peer) {
        return registry.find("raft.replication.match.index").tag("peer", peer).gauge().value();
    }
}
