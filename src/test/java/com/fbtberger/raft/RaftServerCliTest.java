/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The operator's command line.
 *
 * <p>{@code RaftServer} sat at 0 % coverage while carrying every command an operator has: read a
 * key, write one, add or remove a server, add or promote a learner, force a snapshot, ask for
 * status. The argument handling in there is real decision logic — which command was meant,
 * whether it came with the arguments it needs, what to print when the cluster refuses — and none
 * of it was exercised by anything. A typo in the prefix lengths (the {@code regionMatches}
 * offsets) would have shipped.
 *
 * <p>Driven against a REAL single-node cluster rather than a mock. A single node elects itself,
 * so commands actually commit, and what the test reads back is what an operator would see. The
 * mocked alternative would have asserted that this class calls the methods this class calls.
 */
class RaftServerCliTest {

    private RaftNode node;
    private KeyValueStateMachine machine;
    private PrintStream originalOut;
    private ByteArrayOutputStream captured;

    @BeforeEach
    void startNode() throws Exception {
        machine = new KeyValueStateMachine();
        node = new RaftNode(singleNodeConfig(), new InMemoryStorage(), machine,
                addr -> null, RaftMetrics.noop());
        node.start();
        Await.until("the single node has made itself leader", 5_000,
                () -> node.role() == ServerRole.LEADER);

        originalOut = System.out;
        captured = new ByteArrayOutputStream();
        System.setOut(new PrintStream(captured, true, StandardCharsets.UTF_8));
    }

    @AfterEach
    void stopNode() {
        System.setOut(originalOut);
        if (node != null) node.shutdown();
    }

    /** Everything the loop printed, with the capture restored so a failure is readable. */
    private String run(String script) throws Exception {
        RaftServer.runCli(node, machine, new BufferedReader(new StringReader(script)));
        return captured.toString(StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("SET commits and GET reads it back")
    void setThenGet() throws Exception {
        String out = run("SET k v\nGET k\nquit\n");

        assertTrue(out.contains("OK"), out);
        assertTrue(out.lines().anyMatch(l -> l.equals("v")), out);
    }

    @Test
    @DisplayName("A key that was never set says so rather than printing nothing")
    void getMissingKey() throws Exception {
        assertTrue(run("GET nope\nquit\n").contains("(not found)"));
    }

    @Test
    @DisplayName("STATUS names the role, the leader and the configuration")
    void status() throws Exception {
        String out = run("STATUS\nquit\n");

        assertTrue(out.contains("role=LEADER"), out);
        assertTrue(out.contains("configuration="), out);
        assertTrue(out.contains("snapshotIndex="), out);
    }

    @Test
    @DisplayName("SNAPSHOT is taken on demand")
    void snapshot() throws Exception {
        run("SET a 1\nquit\n");
        assertTrue(run("SNAPSHOT\nquit\n").contains("OK"));
    }

    /**
     * The half that a mock would never have caught: these prefixes are matched by LENGTH
     * ({@code regionMatches(true, 0, "ADDLEARNER ", 0, 11)}), so an off-by-one in any of them
     * sends the wrong command or eats a character of the argument.
     */
    @Test
    @DisplayName("A command missing its arguments prints its usage and changes nothing")
    void malformedCommandsPrintUsage() throws Exception {
        String out = run("ADD n2\nADDLEARNER n3\nquit\n");

        assertTrue(out.contains("usage: ADD <id> <host:port>"), out);
        assertTrue(out.contains("usage: ADDLEARNER <id> <host:port>"), out);
        assertFalse(out.contains("OK"), "nothing should have been applied: " + out);
    }

    @Test
    @DisplayName("An unknown command lists the ones that exist")
    void unknownCommand() throws Exception {
        String out = run("FLY ME TO THE MOON\nquit\n");

        assertTrue(out.contains("unknown command"), out);
        assertTrue(out.contains("SNAPSHOT"), "the hint should list the real commands: " + out);
    }

    @Test
    @DisplayName("Commands are case-insensitive and blank lines are ignored")
    void caseAndBlanks() throws Exception {
        String out = run("\n   \nset k2 v2\nget k2\nQUIT\n");

        assertTrue(out.lines().anyMatch(l -> l.equals("v2")), out);
    }

    @Test
    @DisplayName("quit stops the loop, so anything after it is not run")
    void quitStops() throws Exception {
        String out = run("quit\nSET late 1\n");

        assertFalse(out.contains("OK"), "nothing after quit should have run: " + out);
    }

    /** End of input ends the loop just as quit does — an operator closing the pipe. */
    @Test
    @DisplayName("End of input ends the loop")
    void endOfInputStops() throws Exception {
        assertTrue(run("GET k\n").contains("(not found)"));
    }

    // ── describe ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("A refusal from a follower names where to go instead")
    void describeNotLeaderWithHint() {
        String s = RaftServer.describe(new RaftNode.NotLeaderException("n2"));

        assertTrue(s.contains("not leader"), s);
        assertTrue(s.contains("n2"), s);
    }

    /**
     * And when there is no hint, it says so rather than printing "null" — which is what an
     * operator sees during an election, the moment they are most likely to be looking.
     */
    @Test
    @DisplayName("A refusal with no known leader says that, not null")
    void describeNotLeaderWithoutHint() {
        String s = RaftServer.describe(new RaftNode.NotLeaderException(null));

        assertFalse(s.contains("null"), s);
        assertTrue(s.contains("retry"), s);
    }

    @Test
    @DisplayName("Any other failure is reported with its message")
    void describeOther() {
        assertEquals("error: boom", RaftServer.describe(new IllegalStateException("boom")));
    }

    /** A cause with no message must still say something. */
    @Test
    @DisplayName("A failure without a message still names its type")
    void describeMessageless() {
        String s = RaftServer.describe(new IllegalStateException());

        assertFalse(s.endsWith("null"), s);
        assertTrue(s.contains("IllegalStateException"), s);
    }

    private static RaftConfig singleNodeConfig() throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/raft-cli-test-unused");
        props.setProperty("peer.n1", "localhost:9091");
        props.setProperty("snapshot.threshold", "100");
        Path tmp = Files.createTempFile("raft-cli-", ".properties");
        try (var out = Files.newOutputStream(tmp)) { props.store(out, null); }
        return RaftConfig.load(tmp);
    }
}
