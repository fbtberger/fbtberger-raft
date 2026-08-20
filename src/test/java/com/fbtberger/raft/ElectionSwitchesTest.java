/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The switch <em>configuration</em>: what an unconfigured node runs, and what it takes to arm a
 * defect. The behaviour each switch changes is in {@link ElectionDefectSwitchTest}.
 *
 * <p>The load-bearing property here is the default. A node whose properties file says nothing --
 * every existing deployment, every test, every {@code RaftConfig.of} caller -- must come up fixed.
 */
class ElectionSwitchesTest {

    @Test
    void anEmptyConfigurationIsTheFixedConfiguration() {
        ElectionSwitches switches = ElectionSwitches.fromProperties(new Properties());

        assertTrue(switches.preVoteQuorumLatch());
        assertEquals(6, switches.electionBootDelayFactor());
        assertTrue(switches.leaderStickiness());
        assertTrue(switches.allFixed(), "an unconfigured node must have no defect armed");
    }

    /** The programmatic factory has no switch parameters at all, so it cannot arm one by accident. */
    @Test
    void theProgrammaticFactoryProducesAFixedConfiguration() {
        RaftConfig config = RaftConfig.of("n1", 9091, java.nio.file.Path.of("/tmp/raft-switches-n1"),
                java.util.Map.of("n1", "localhost:9091", "n2", "localhost:9092", "n3", "localhost:9093"));

        assertTrue(config.electionSwitches().allFixed());
        assertFalse(config.withElectionSwitches(new ElectionSwitches(false, 1, false))
                .electionSwitches().allFixed(), "withElectionSwitches must actually replace them");
    }

    @Test
    void eachSwitchIsReadFromTheProperties() {
        Properties props = new Properties();
        props.setProperty(ElectionSwitches.KEY_QUORUM_LATCH, "false");
        props.setProperty(ElectionSwitches.KEY_BOOT_DELAY_FACTOR, "1");
        props.setProperty(ElectionSwitches.KEY_LEADER_STICKINESS, "false");

        ElectionSwitches switches = ElectionSwitches.fromProperties(props);

        assertFalse(switches.preVoteQuorumLatch());
        assertEquals(1, switches.electionBootDelayFactor());
        assertFalse(switches.leaderStickiness());
        assertFalse(switches.allFixed(), "three armed defects are not a default configuration");
    }

    /**
     * The property that lets both talks ship one artifact and one configuration file: the defect
     * for the evening is chosen on the command line, not by editing the Pi's properties.
     */
    @Test
    void aSystemPropertyOverridesTheFile() {
        Properties props = new Properties();
        props.setProperty(ElectionSwitches.KEY_BOOT_DELAY_FACTOR, "6");
        System.setProperty(ElectionSwitches.KEY_BOOT_DELAY_FACTOR, "1");
        try {
            assertEquals(1, ElectionSwitches.fromProperties(props).electionBootDelayFactor());
        } finally {
            System.clearProperty(ElectionSwitches.KEY_BOOT_DELAY_FACTOR);
        }
    }

    /**
     * {@code Boolean.parseBoolean} maps every typo to {@code false}, and {@code false} on these
     * keys means "defect armed". A misspelled value must stop the node, not silently arm a bug.
     */
    @Test
    void aTypoInABooleanSwitchIsRejectedRatherThanReadAsFalse() {
        Properties props = new Properties();
        props.setProperty(ElectionSwitches.KEY_QUORUM_LATCH, "ture");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ElectionSwitches.fromProperties(props));
        assertTrue(e.getMessage().contains(ElectionSwitches.KEY_QUORUM_LATCH), e.getMessage());
    }

    @Test
    void aBootDelayFactorBelowOneIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new ElectionSwitches(true, 0, true));
        assertThrows(IllegalArgumentException.class,
                () -> new ElectionSwitches(true, -1, true));
    }

    /** The startup log line has to name every switch, or a demo trace cannot be read later. */
    @Test
    void toStringNamesEverySwitch() {
        String rendered = ElectionSwitches.defaults().toString();

        assertTrue(rendered.contains(ElectionSwitches.KEY_QUORUM_LATCH), rendered);
        assertTrue(rendered.contains(ElectionSwitches.KEY_BOOT_DELAY_FACTOR), rendered);
        assertTrue(rendered.contains(ElectionSwitches.KEY_LEADER_STICKINESS), rendered);
    }
}
