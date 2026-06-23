package com.fbtberger.raft.client;

import com.fbtberger.raft.RaftConfig;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * A minimal standalone client process: unlike {@code RaftServer}'s embedded CLI
 * (which talks to its own in-process {@code RaftNode} directly), this runs as a
 * separate process that knows nothing about Raft internals -- only the cluster's
 * address book and the {@link RaftClient}/{@link KeyValueClient} layer. It exists
 * to demonstrate that the client layer is genuinely independent of any one server
 * process and can be embedded in arbitrary client applications.
 * <p>
 * Any one of the cluster's own {@code node*.properties} files works as the source
 * of cluster topology here, since they all list the same set of peer addresses --
 * this demo only reads {@code peerAddresses()} from it, ignoring the per-node
 * {@code node.id} / {@code node.port} / {@code data.dir} fields that are only
 * meaningful to a server process.
 * <p>
 * {@code ADD}/{@code REMOVE} drive §6 cluster reconfiguration through
 * {@link RaftClient#addServer} / {@link RaftClient#removeServer}. Since this
 * demo's address book is read once at startup from the given properties file, a
 * server added after that won't be directly reachable from here until you also
 * add it to that file and restart -- see {@link RaftClient}'s class doc for why.
 * <p>
 * Usage: {@code java -cp raft-java.jar com.fbtberger.raft.client.RaftClientDemo config/node1.properties}
 */
public final class RaftClientDemo {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: java -cp raft-java.jar com.fbtberger.raft.client.RaftClientDemo <path-to-any-node.properties>");
            System.exit(1);
        }

        RaftConfig config = RaftConfig.load(Path.of(args[0]));

        try (RaftClient raftClient = new RaftClient(config.peerAddresses())) {
            KeyValueClient kvClient = new KeyValueClient(raftClient);
            System.out.println("Connected to cluster: " + config.peerAddresses().keySet());
            System.out.println("Commands: SET <key> <value> | ADD <id> <host:port> | REMOVE <id> | quit");

            BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                if (line.equalsIgnoreCase("quit")) {
                    return;
                }
                if (line.regionMatches(true, 0, "SET ", 0, 4)) {
                    String[] parts = line.substring(4).trim().split("\\s+", 2);
                    if (parts.length != 2) {
                        System.out.println("usage: SET <key> <value>");
                        continue;
                    }
                    try {
                        System.out.println(kvClient.set(parts[0], parts[1]));
                    } catch (RaftClientException e) {
                        System.out.println("error: " + e.getMessage());
                    }
                } else if (line.regionMatches(true, 0, "ADD ", 0, 4)) {
                    String[] parts = line.substring(4).trim().split("\\s+", 2);
                    if (parts.length != 2) {
                        System.out.println("usage: ADD <id> <host:port>");
                        continue;
                    }
                    try {
                        raftClient.addServer(parts[0], parts[1]);
                        System.out.println("OK");
                    } catch (RaftClientException e) {
                        System.out.println("error: " + e.getMessage());
                    }
                } else if (line.regionMatches(true, 0, "REMOVE ", 0, 7)) {
                    String id = line.substring(7).trim();
                    try {
                        raftClient.removeServer(id);
                        System.out.println("OK");
                    } catch (RaftClientException e) {
                        System.out.println("error: " + e.getMessage());
                    }
                } else {
                    System.out.println("unknown command, try SET <key> <value> | ADD <id> <host:port> | REMOVE <id> | quit");
                }
            }
        }
    }
}
