package com.fbtberger.raft;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Process entry point. Sets up the Spring IoC container from
 * {@link RaftNodeConfiguration}, which wires every collaborator
 * ({@link RaftConfig}, {@link RaftStorage}, {@link StateMachine},
 * {@link RaftNode}, the gRPC services) without this class needing to know
 * how any of them are constructed. After the context is ready, this class
 * arms the node's election timer, runs the interactive CLI, and then
 * closes the Spring context (which triggers the registered
 * {@code destroyMethod}s in the right order: gRPC server → Raft node →
 * storage).
 *
 * <p>Usage: {@code java -jar raft-java.jar config/node1.properties}
 */
public final class RaftServer {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: java -jar raft-java.jar <path-to-node.properties>");
            System.exit(1);
        }

        // Pass the config-file path to Spring via a system property so that
        // RaftNodeConfiguration's @Value("${raft.config.path}") can pick it up
        // without requiring Spring Boot's full property-source machinery.
        System.setProperty("raft.config.path", args[0]);

        AnnotationConfigApplicationContext ctx =
                new AnnotationConfigApplicationContext(RaftNodeConfiguration.class);

        // All dependencies are already wired by the context; just retrieve
        // the two beans this class needs to interact with directly.
        RaftNode             raftNode     = ctx.getBean(RaftNode.class);
        RaftConfig           config       = ctx.getBean(RaftConfig.class);
        KeyValueStateMachine stateMachine = ctx.getBean(KeyValueStateMachine.class);

        // Arm the election timer now that the gRPC server is confirmed
        // running (the grpcServer bean is started inside the context).
        raftNode.start();

        System.out.println("[" + config.selfId() + "] listening on port " + config.selfPort()
                + ", starting configuration=" + config.peerAddresses().keySet());

        // Ensure Spring destroys all beans (grpcServer.shutdown() →
        // raftNode.shutdown() → raftStorage.close()) whether we exit via
        // the CLI "quit" command, a SIGTERM, or an uncaught exception.
        Runtime.getRuntime().addShutdownHook(new Thread(ctx::close));

        runCli(raftNode, stateMachine);

        // CLI loop exited normally ("quit") — close the context explicitly
        // so we don't rely solely on the shutdown hook.
        ctx.close();
    }

    /**
     * Minimal interactive command loop for the demo key-value store built on
     * top of Raft. Supported commands:
     * <ul>
     *   <li>{@code SET <key> <value>} — submits a command through Raft and waits for it to commit</li>
     *   <li>{@code GET <key>} — reads directly from the local (possibly stale, non-linearizable) state machine</li>
     *   <li>{@code ADD <id> <host:port>} — adds a new voting member (§6)</li>
     *   <li>{@code REMOVE <id>} — removes an existing voting member (§6)</li>
     *   <li>{@code SNAPSHOT} — forces an immediate log-compaction snapshot (§7)</li>
     *   <li>{@code STATUS} — prints this node's current role, known leader, configuration,
     *       and snapshot boundary</li>
     *   <li>{@code quit} — exits the process</li>
     * </ul>
     */
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
                } else if (line.equalsIgnoreCase("SNAPSHOT")) {
                    raftNode.snapshotNow();
                    System.out.println("OK");
                } else {
                    System.out.println("unknown command, try SET <key> <value> | GET <key>"
                            + " | ADD <id> <host:port> | REMOVE <id> | SNAPSHOT | STATUS | quit");
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
