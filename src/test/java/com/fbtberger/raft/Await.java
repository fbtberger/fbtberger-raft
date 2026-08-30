package com.fbtberger.raft;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Wait for a condition instead of sleeping for a guess.
 *
 * <p>Almost every test in this repository drives a cluster and then wants to know that something
 * has happened — a leader exists, a snapshot fired, a command was refused. The habit was
 * {@code Thread.sleep(n)} followed by an assertion, and it is wrong twice over:
 *
 * <ul>
 *   <li><b>Too short and it fails for no reason.</b> The sleep encodes a guess about how fast the
 *       machine is, and a loaded CI box or a laptop on battery makes that guess wrong.</li>
 *   <li><b>Too long and the suite is slow</b> — every test pays the worst case on every run, even
 *       though the condition is usually true within milliseconds.</li>
 * </ul>
 *
 * <p>There is a third cost, and it is the one that started this: a sleep does not say WHEN the
 * test is finished, so the code paths that ran by the time it returns differ from run to run.
 * Measured on 2026-08-30 across identical suites — 344 tests, no failures, no skips — line
 * coverage moved between 4199 and 4214, entirely inside passing tests. Ten of those lines were
 * the gRPC transport's own {@code requestVote} and {@code appendEntries} handlers: whether a
 * follower had answered yet by the time the assertion was satisfied was a coin toss. A ratchet
 * cannot be set against a number that moves for that reason.
 *
 * <p>Deliberately not Awaitility, which some tests here already use: this is fifteen lines, it
 * needs no configuration, and it keeps the failure message in the test's own words.
 */
final class Await {

    /** How often to look. Short enough that a fast condition costs nothing worth measuring. */
    private static final long POLL_MILLIS = 10;

    private Await() {}

    /**
     * Block until {@code condition} holds.
     *
     * @param what what is being waited for, for the failure message — write it as the thing that
     *             did not happen ("a leader is elected"), because that is what a red build shows
     * @throws AssertionError if it has not happened within {@code timeoutMillis}
     */
    static void until(String what, long timeoutMillis, BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(POLL_MILLIS);
        }
        // One last look: the loop may have exited on the deadline just as it became true.
        if (condition.getAsBoolean()) return;
        fail("timed out after " + timeoutMillis + " ms waiting until " + what);
    }

    /**
     * Block until {@code value} returns non-null, and give it back.
     *
     * <p>The same wait, for the common case where the test needs the thing it waited for rather
     * than only the fact that it exists.
     */
    static <T> T until(String what, long timeoutMillis, Supplier<T> value)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
        T last = null;
        while (System.nanoTime() < deadline) {
            last = value.get();
            if (last != null) return last;
            Thread.sleep(POLL_MILLIS);
        }
        last = value.get();
        if (last != null) return last;
        fail("timed out after " + timeoutMillis + " ms waiting until " + what);
        return null;   // unreachable; fail() throws
    }
}
