/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class RaftServer {

    private static final Logger LOG = LoggerFactory.getLogger(RaftServer.class);

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            LOG.error("Usage: java -jar fbtberger-raft.jar <path-to-node.properties>");
            System.exit(1);
        }

        System.setProperty("raft.config.path", args[0]);

        AnnotationConfigApplicationContext ctx =
                new AnnotationConfigApplicationContext(RaftNodeConfiguration.class);

        RaftNode             raftNode     = ctx.getBean(RaftNode.class);
        RaftConfig           config       = ctx.getBean(RaftConfig.class);
        KeyValueStateMachine stateMachine = ctx.getBean(KeyValueStateMachine.class);

        raftNode.start();

        LOG.info("[{}] listening on port {}, starting configuration={}",
                config.selfId(), config.selfPort(), config.peerAddresses().keySet());
        if (config.metricsPort() > 0) {
            LOG.info("[{}] Prometheus metrics at http://localhost:{}/metrics",
                    config.selfId(), config.metricsPort());
        }

        Runtime.getRuntime().addShutdownHook(new Thread(ctx::close));

        runCli(raftNode, stateMachine);

        ctx.close();
    }

    private static void runCli(RaftNode raftNode, KeyValueStateMachine stateMachine) throws Exception {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String line;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) continue;
            if (line.equalsIgnoreCase("quit")) return;
            try {
                if (line.equalsIgnoreCase("STATUS")) {
                    System.out.println("role=" + raftNode.role() + " leader=" + raftNode.currentLeaderId()
                            + " configuration=" + raftNode.currentConfiguration().keySet()
                            + " learners=" + raftNode.currentLearners().keySet()
                            + " snapshotIndex=" + raftNode.snapshotIndex());
                } else if (line.regionMatches(true, 0, "GET ", 0, 4)) {
                    String value = stateMachine.get(line.substring(4).trim());
                    System.out.println(value == null ? "(not found)" : value);
                } else if (line.regionMatches(true, 0, "SET ", 0, 4)) {
                    byte[] result = raftNode.submitCommand(line.getBytes(StandardCharsets.UTF_8))
                            .get(2, TimeUnit.SECONDS);
                    System.out.println(new String(result, StandardCharsets.UTF_8));
                } else if (line.regionMatches(true, 0, "ADD ", 0, 4)) {
                    String[] parts = line.substring(4).trim().split("\\s+", 2);
                    if (parts.length != 2) { System.out.println("usage: ADD <id> <host:port>"); continue; }
                    raftNode.addServer(parts[0], parts[1]).get(2, TimeUnit.SECONDS);
                    System.out.println("OK");
                } else if (line.regionMatches(true, 0, "REMOVE ", 0, 7)) {
                    raftNode.removeServer(line.substring(7).trim()).get(2, TimeUnit.SECONDS);
                    System.out.println("OK");
                } else if (line.regionMatches(true, 0, "ADDLEARNER ", 0, 11)) {
                    String[] parts = line.substring(11).trim().split("\\s+", 2);
                    if (parts.length != 2) { System.out.println("usage: ADDLEARNER <id> <host:port>"); continue; }
                    raftNode.addLearner(parts[0], parts[1]).get(2, TimeUnit.SECONDS);
                    System.out.println("OK");
                } else if (line.regionMatches(true, 0, "PROMOTE ", 0, 8)) {
                    raftNode.promoteLearner(line.substring(8).trim()).get(2, TimeUnit.SECONDS);
                    System.out.println("OK");
                } else if (line.regionMatches(true, 0, "REMOVELEARNER ", 0, 14)) {
                    raftNode.removeLearner(line.substring(14).trim()).get(2, TimeUnit.SECONDS);
                    System.out.println("OK");
                } else if (line.equalsIgnoreCase("SNAPSHOT")) {
                    raftNode.snapshotNow();
                    System.out.println("OK");
                } else {
                    System.out.println("unknown command, try SET <key> <value> | GET <key>"
                            + " | ADD <id> <host:port> | REMOVE <id>"
                            + " | ADDLEARNER <id> <host:port> | PROMOTE <id> | REMOVELEARNER <id>"
                            + " | SNAPSHOT | STATUS | quit");
                }
            } catch (ExecutionException e) {
                System.out.println(describe(e.getCause()));
            } catch (TimeoutException e) {
                System.out.println("timed out waiting for commit (no leader / no majority?)");
            }
        }
    }

    private static String describe(Throwable cause) {
        if (cause instanceof RaftNode.NotLeaderException nle) {
            return "not leader; try " + (nle.leaderHint != null ? nle.leaderHint : "(unknown leader, retry shortly)");
        }
        return "error: " + (cause.getMessage() != null ? cause.getMessage() : cause);
    }
}
