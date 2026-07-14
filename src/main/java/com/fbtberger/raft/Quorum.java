/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.util.Map;
import java.util.Set;

/**
 * The one answer to "do these acknowledgements amount to a quorum?" — including during a
 * configuration change (§4.3).
 *
 * <h2>Why this class exists</h2>
 * {@link RaftNode} used to answer that question in <b>three</b> places, and two of them were wrong:
 *
 * <ul>
 *   <li>{@code advanceCommitIndex} asked it correctly, via separate majorities in
 *       C<sub>old</sub> <em>and</em> C<sub>new</sub>;</li>
 *   <li>the <b>ReadIndex barrier</b> (§6.4) compared the size of the acknowledgement set against a
 *       single number, {@code max(|Cold|/2+1, |Cnew|/2+1)};</li>
 *   <li>{@code hasValidLease()} counted acknowledgements <b>only within C<sub>new</sub></b> and
 *       ignored C<sub>old</sub> altogether.</li>
 * </ul>
 *
 * <h2>Why a single threshold is not enough</h2>
 * Joint consensus does not ask "how many servers acknowledged?" — it asks "did a majority of
 * C<sub>old</sub> acknowledge, <em>and</em> a majority of C<sub>new</sub>?". Those are different
 * questions, and a count over the union cannot express the second one. Concretely, with
 * C<sub>old</sub> = {a, b, c}, C<sub>new</sub> = {a, d, e} and {@code a} as leader:
 *
 * <pre>
 *   single threshold: max(2, 2) = 2  ->  a + d is "enough"
 *   Cnew majority:    {a, d} = 2/3   ->  satisfied
 *   Cold majority:    {a}    = 1/3   ->  NOT satisfied
 * </pre>
 *
 * So a leader could confirm its leadership — and serve a supposedly linearizable read, or report a
 * healthy lease — with no majority in C<sub>old</sub> behind it at all. During a membership change
 * that is precisely the guarantee the ReadIndex protocol exists to provide.
 *
 * <h2>Why it is a pure function</h2>
 * A quorum decision that lives inside a node, spread across three call sites, cannot be tested
 * without standing up a cluster and racing it into a configuration change — which is exactly why
 * two of the three were wrong and nobody noticed. As a pure function over (acknowledgements,
 * C<sub>old</sub>, C<sub>new</sub>) it is a handful of assertions.
 */
final class Quorum {

    private Quorum() { }

    /**
     * Do {@code acks} constitute a quorum of {@code current} — and, if a configuration change is in
     * flight, of {@code old} as well?
     *
     * @param acks    the ids that acknowledged. The leader must include itself; it is not added
     *                implicitly, because a leader being <em>removed</em> by the change is not a
     *                member of C<sub>new</sub> and must not be counted there.
     * @param current C<sub>new</sub> — the configuration now in effect ("id" -> "host:port")
     * @param old     C<sub>old</sub>, or {@code null} when no change is in flight
     */
    static boolean reached(Set<String> acks, Map<String, String> current, Map<String, String> old) {
        if (!hasMajority(acks, current)) return false;
        if (old == null) return true;
        return hasMajority(acks, old);
    }

    /** The number of acknowledgements a majority of {@code config} requires. */
    static int majorityOf(Map<String, String> config) {
        return config.size() / 2 + 1;
    }

    private static boolean hasMajority(Set<String> acks, Map<String, String> config) {
        int count = 0;
        for (String member : config.keySet()) {
            // Only members of THIS configuration count towards ITS majority. An acknowledgement
            // from a learner, or from a server that belongs only to the other configuration, is
            // not a vote here.
            if (acks.contains(member)) count++;
        }
        return count >= majorityOf(config);
    }
}
