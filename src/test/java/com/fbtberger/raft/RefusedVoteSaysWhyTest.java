/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A vote that is refused says why it was refused.
 *
 * <p>RequestVote used to grant with a log line and refuse without one. On 2026-11-25 a
 * leadership transfer on the Pi cluster failed that way: the target campaigned, all five
 * voters adopted its term, not one granted it a vote, and no node recorded a reason --
 * the fault had to be reconstructed from timestamps and replication state. The PreVote
 * handler had already been taught this in v122, in the words of issue #2: the side that
 * knows the reason was the silent one.
 *
 * <p>So the refusal names the two things that decide it -- whether this node may still vote
 * in the term, and whether the candidate's log is far enough along -- and the logs they are
 * compared on, because "logOk=false" without the two positions only moves the question.
 */
class RefusedVoteSaysWhyTest {

    private RaftNode node;
    private Capture capture;

    @BeforeEach
    void attach() {
        capture = new Capture();
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(RaftNode.class);
        capture.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        capture.start();
        logger.addAppender(capture);
        logger.setLevel(Level.INFO);
    }

    @AfterEach
    void detach() {
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(RaftNode.class))
                .detachAppender(capture);
        if (node != null) {
            node.shutdown();
        }
    }

    @Test
    void aVoteRefusedOnAStaleLogSaysSoAndSaysWhichLogs() throws Exception {
        node = started();

        // A candidate whose log is behind: nothing at all in it, while this node has entries.
        // Term 99 is far ahead of anything this node has reached, so the request is not
        // refused for being stale -- only the log can decide it.
        // The transfer flag is set, so the refusal cannot be stickiness -- which is exactly
        // the case that was impossible to tell apart without this line.
        RequestVoteResponse response = node.handleRequestVote(RequestVoteRequest.newBuilder()
                .setTerm(99).setCandidateId("n2")
                .setLastLogIndex(0).setLastLogTerm(0)
                .setLeadershipTransfer(true).build());

        assertFalse(response.getVoteGranted(), "a stale candidate must not be elected");
        String said = capture.lines.stream()
                .filter(l -> l.contains("denied vote to n2"))
                .findFirst()
                .orElse("");
        assertFalse(said.isEmpty(), "the refusal was silent:\n" + String.join("\n", capture.lines));
        assertTrue(said.contains("logOk=false"), said);
        assertTrue(said.contains("mine=") && said.contains("theirs="), said);
    }

    private RaftNode started() throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9391");
        props.setProperty("data.dir", "/tmp/raft-refused-vote-unused");
        props.setProperty("peer.n1", "localhost:9391");
        props.setProperty("snapshot.threshold", "1000000");
        Path tmp = Files.createTempFile("raft-refused-vote-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        RaftNode n = new RaftNode(RaftConfig.load(tmp), new InMemoryStorage(),
                new KeyValueStateMachine(), addr -> null, RaftMetrics.noop());
        n.start();
        long deadline = System.currentTimeMillis() + 3_000;
        while (n.role() != ServerRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        return n;
    }

    /** Keeps what RaftNode logged, as rendered text. */
    private static final class Capture extends AppenderBase<ILoggingEvent> {
        private final List<String> lines = new CopyOnWriteArrayList<>();

        @Override
        protected void append(ILoggingEvent event) {
            lines.add(event.getFormattedMessage());
        }
    }
}
