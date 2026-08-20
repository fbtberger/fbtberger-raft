/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.util.Properties;

/**
 * Runtime switches that can put two historic election defects back into a running node.
 *
 * <p>They exist for one reason: a talk that claims "this bug cost us a leader on every restart"
 * is more convincing when the bug can be shown happening than when it is described. Every switch
 * therefore has a safe default -- the fixed behaviour -- and turning it off restores what the
 * node did before the fix. Nothing here is a code branch a consumer is meant to choose between:
 * <b>production runs the defaults</b>, and a node that logs anything other than the defaults at
 * startup is a node someone is demonstrating on.
 *
 * <table>
 *   <caption>The switches</caption>
 *   <tr><th>Key</th><th>Default</th><th>Off / low restores</th></tr>
 *   <tr>
 *     <td>{@value #KEY_QUORUM_LATCH}</td><td>{@code true}</td>
 *     <td>issue #3 -- the PreVote quorum handler fires once per grant instead of once per
 *         round, so a round that two peers grant runs two full elections</td>
 *   </tr>
 *   <tr>
 *     <td>{@value #KEY_BOOT_DELAY_FACTOR}</td><td>{@value #DEFAULT_BOOT_DELAY_FACTOR}</td>
 *     <td>{@code 1} -- a freshly started node campaigns after one ordinary election timeout
 *         (150-300 ms) instead of waiting out the window in which a live leader's transport
 *         reconnects to it</td>
 *   </tr>
 *   <tr>
 *     <td>{@value #KEY_LEADER_STICKINESS}</td><td>{@code true}</td>
 *     <td>issue #2 -- a LEADER no longer counts as having heard from a leader, so it grants
 *         (pre-)votes to a node that is trying to unseat it</td>
 *   </tr>
 * </table>
 *
 * <p><b>Why issue #2 needs two of them.</b> The boot delay is the requester side: it decides
 * whether a restarting node campaigns at all. Leader stickiness is the responder side, and it is
 * the side the defect actually lived on -- with three voters the quorum is two, the candidate
 * counts itself, so the incumbent's own grant was enough to unseat it. Leaving stickiness on
 * while setting the boot delay to 1 produces a node that campaigns and is refused by everyone:
 * visible in the log, but no leader change. Both must be lowered to reproduce the restart that
 * cost a leadership.
 *
 * <p>Values come from the node's {@code .properties} file, and a {@code -D} system property of
 * the same name overrides it. That is what lets both talks run the same artifact and the same
 * configuration file, flipping a defect on for one run and off for the next.
 */
public final class ElectionSwitches {

    public static final String KEY_QUORUM_LATCH = "raft.prevote.quorum-latch";
    public static final String KEY_BOOT_DELAY_FACTOR = "raft.election.boot-delay-factor";
    public static final String KEY_LEADER_STICKINESS = "raft.prevote.leader-stickiness";

    public static final boolean DEFAULT_QUORUM_LATCH = true;
    public static final int DEFAULT_BOOT_DELAY_FACTOR = 6;
    public static final boolean DEFAULT_LEADER_STICKINESS = true;

    private final boolean preVoteQuorumLatch;
    private final int electionBootDelayFactor;
    private final boolean leaderStickiness;

    public ElectionSwitches(boolean preVoteQuorumLatch, int electionBootDelayFactor,
                            boolean leaderStickiness) {
        if (electionBootDelayFactor < 1) {
            // 0 would arm a timer that fires immediately and campaign before the transport is
            // even up; a negative value reaches Random.nextInt with a non-positive bound and
            // throws inside the scheduler, where the exception is swallowed silently.
            throw new IllegalArgumentException(
                    KEY_BOOT_DELAY_FACTOR + " must be >= 1, was " + electionBootDelayFactor);
        }
        this.preVoteQuorumLatch = preVoteQuorumLatch;
        this.electionBootDelayFactor = electionBootDelayFactor;
        this.leaderStickiness = leaderStickiness;
    }

    /** Every defect fixed -- what production runs, and what a node uses if nothing is set. */
    public static ElectionSwitches defaults() {
        return new ElectionSwitches(DEFAULT_QUORUM_LATCH, DEFAULT_BOOT_DELAY_FACTOR,
                DEFAULT_LEADER_STICKINESS);
    }

    /**
     * Reads the three keys from {@code props}, with a system property of the same name taking
     * precedence -- see the class javadoc for why the override exists.
     */
    public static ElectionSwitches fromProperties(Properties props) {
        return new ElectionSwitches(
                parseBoolean(props, KEY_QUORUM_LATCH, DEFAULT_QUORUM_LATCH),
                parseInt(props, KEY_BOOT_DELAY_FACTOR, DEFAULT_BOOT_DELAY_FACTOR),
                parseBoolean(props, KEY_LEADER_STICKINESS, DEFAULT_LEADER_STICKINESS));
    }

    private static String value(Properties props, String key) {
        String override = System.getProperty(key);
        return override != null ? override : props.getProperty(key);
    }

    private static boolean parseBoolean(Properties props, String key, boolean defaultValue) {
        String v = value(props, key);
        if (v == null) return defaultValue;
        // Boolean.parseBoolean turns every typo into `false`, which for these keys means
        // "defect on" -- the one outcome nobody would choose by accident.
        String trimmed = v.trim();
        if (trimmed.equalsIgnoreCase("true")) return true;
        if (trimmed.equalsIgnoreCase("false")) return false;
        throw new IllegalArgumentException(key + " must be true or false, was '" + v + "'");
    }

    private static int parseInt(Properties props, String key, int defaultValue) {
        String v = value(props, key);
        return v == null ? defaultValue : Integer.parseInt(v.trim());
    }

    /** Whether a decided PreVote round is consumed, so its later grants are dropped (issue #3). */
    public boolean preVoteQuorumLatch() { return preVoteQuorumLatch; }

    /**
     * How many ordinary election timeouts a node waits before its <em>first</em> campaign after
     * {@link RaftNode#start()}. Only the first timer is scaled; once anything resets the timer,
     * the node is on the normal 150-300 ms schedule.
     */
    public int electionBootDelayFactor() { return electionBootDelayFactor; }

    /** Whether a LEADER counts as having leader contact when answering (pre-)votes (issue #2). */
    public boolean leaderStickiness() { return leaderStickiness; }

    /** Whether every switch is at its default -- i.e. no defect is armed. */
    public boolean allFixed() {
        return preVoteQuorumLatch == DEFAULT_QUORUM_LATCH
                && electionBootDelayFactor == DEFAULT_BOOT_DELAY_FACTOR
                && leaderStickiness == DEFAULT_LEADER_STICKINESS;
    }

    /** Rendered into the node's startup log line, so a demo run says so in its own trace. */
    @Override
    public String toString() {
        return KEY_QUORUM_LATCH + "=" + preVoteQuorumLatch
                + ", " + KEY_BOOT_DELAY_FACTOR + "=" + electionBootDelayFactor
                + ", " + KEY_LEADER_STICKINESS + "=" + leaderStickiness;
    }
}
