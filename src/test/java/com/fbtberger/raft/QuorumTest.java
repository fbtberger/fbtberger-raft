/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The quorum predicate — the question {@link RaftNode} used to answer in three places, two of them
 * wrongly.
 *
 * <p>{@code advanceCommitIndex} got it right (separate majorities in C_old and C_new). The
 * ReadIndex barrier compared the size of the acknowledgement set against a single number, and
 * {@code hasValidLease()} looked only at C_new. Both were correct while no configuration change was
 * in flight, which is why nothing ever failed: <b>the cluster simply never spent time in joint
 * consensus.</b> The same reason {@code truncateFrom} survived for months.
 *
 * <p>These tests are cheap because the decision is now a pure function. That is the point: a quorum
 * rule buried in three call sites can only be tested by standing up a cluster and racing it into a
 * membership change — so in practice it was not tested at all.
 */
class QuorumTest {

    private static final Map<String, String> COLD =
            Map.of("a", "h:1", "b", "h:2", "c", "h:3");
    private static final Map<String, String> CNEW =
            Map.of("a", "h:1", "d", "h:4", "e", "h:5");

    // ── steady state (no configuration change) ───────────────────────────────

    @Test
    void aMajorityOfTheOnlyConfigurationIsAQuorum() {
        assertTrue(Quorum.reached(Set.of("a", "b"), COLD, null));
    }

    @Test
    void aMinorityIsNot() {
        assertFalse(Quorum.reached(Set.of("a"), COLD, null));
    }

    @Test
    void aSingleNodeClusterIsItsOwnQuorum() {
        Map<String, String> solo = Map.of("a", "h:1");
        assertTrue(Quorum.reached(Set.of("a"), solo, null));
        assertFalse(Quorum.reached(Set.of(), solo, null));
    }

    @Test
    void anAcknowledgementFromANonMemberDoesNotCount() {
        // A learner (§4.2.1) is not in the configuration, so its ack is not a vote. This is what
        // stops a leader that has lost its voters from confirming its leadership with non-voters.
        assertFalse(Quorum.reached(Set.of("a", "learner1", "learner2"), COLD, null));
    }

    // ── joint consensus (§4.3) — THE BUG ─────────────────────────────────────

    /**
     * The counterexample. C_old = {a,b,c}, C_new = {a,d,e}, leader = a.
     *
     * <pre>
     *   old single-threshold rule: max(2, 2) = 2  ->  {a, d} was "enough"
     *   C_new majority: {a, d} = 2 of 3           ->  satisfied
     *   C_old majority: {a}    = 1 of 3           ->  NOT satisfied
     * </pre>
     *
     * The leader would have confirmed its leadership — and served a supposedly linearizable read,
     * and reported a healthy lease — with no majority of C_old behind it at all.
     */
    @Test
    void aMajorityOfTheNewConfigurationAloneIsNotAQuorum() {
        assertFalse(Quorum.reached(Set.of("a", "d"), CNEW, COLD),
                "C_new has a majority, C_old does not — this is not a quorum");
    }

    @Test
    void aMajorityOfTheOldConfigurationAloneIsNotAQuorumEither() {
        assertFalse(Quorum.reached(Set.of("a", "b"), CNEW, COLD),
                "and the mistake is symmetric");
    }

    @Test
    void bothMajoritiesTogetherAreAQuorum() {
        // {a,b} is a majority of C_old; {a,d} is a majority of C_new.
        assertTrue(Quorum.reached(Set.of("a", "b", "d"), CNEW, COLD));
    }

    @Test
    void theOverlappingServerCountsInBothConfigurations() {
        // 'a' belongs to both, so it contributes to both majorities — that is exactly what makes
        // the two quorums intersect, and what joint consensus relies on.
        assertTrue(Quorum.reached(Set.of("a", "c", "e"), CNEW, COLD));
    }

    /**
     * A leader that is REMOVING itself is not a member of C_new, so it cannot count towards C_new's
     * majority. This is why {@link Quorum#reached} does not add "self" implicitly — the caller must
     * pass it, and it is only counted where it is actually a member.
     */
    @Test
    void aLeaderBeingRemovedDoesNotCountTowardsTheNewConfiguration() {
        Map<String, String> withoutA = Map.of("d", "h:4", "e", "h:5");

        assertFalse(Quorum.reached(Set.of("a", "b", "d"), withoutA, COLD),
                "'a' is not in C_new; only 'd' is, and one of two is not a majority");
        assertTrue(Quorum.reached(Set.of("a", "b", "d", "e"), withoutA, COLD));
    }

    @Test
    void aQuorumIsNeverReachedByCountingAlone() {
        // Three acknowledgements, and still not a quorum: b and c are not in C_new, and d alone is
        // not a majority of it. Size is not the question; membership is.
        assertEquals(3, Set.of("a", "b", "c").size());
        assertFalse(Quorum.reached(Set.of("a", "b", "c"), CNEW, COLD));
    }

    // ── the arithmetic itself ────────────────────────────────────────────────

    @Test
    void majorityOfAnEvenConfigurationIsMoreThanHalf() {
        assertEquals(3, Quorum.majorityOf(Map.of("a", "1", "b", "2", "c", "3", "d", "4")));
        assertEquals(2, Quorum.majorityOf(Map.of("a", "1", "b", "2", "c", "3")));
        assertEquals(1, Quorum.majorityOf(Map.of("a", "1")));
    }
}
