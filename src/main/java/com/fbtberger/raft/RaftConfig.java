package com.fbtberger.raft;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Static cluster and node configuration, loaded from a plain .properties
 * file.
 *
 * Example file:
 *   node.id=node1
 *   node.port=9091
 *   data.dir=/var/raft/node1
 *   peer.node1=localhost:9091
 *   peer.node2=localhost:9092
 *   peer.node3=localhost:9093
 *
 * The peer list must include this node's own id/address; that's how the
 * cluster size (and therefore the majority needed for votes and commits,
 * §5.2) is determined.
 *
 * <p>Note this only describes the <em>bootstrap</em> configuration a node
 * starts up with. Once §6 cluster reconfiguration is used (see
 * {@link RaftNode#addServer} / {@link RaftNode#removeServer}), the live
 * membership and the majority computed from it are tracked dynamically
 * inside {@link RaftNode} from whatever configuration entry is latest in its
 * log -- {@link #peerAddresses()}, {@link #clusterSize()} and
 * {@link #majority()} below keep returning the original file's contents and
 * no longer reflect the running cluster's actual membership after that.
 */
public final class RaftConfig {

    private static final int DEFAULT_SNAPSHOT_THRESHOLD = 100;
    private static final int DEFAULT_SNAPSHOT_CHUNK_SIZE = 1_048_576; // 1 MB

    private final String selfId;
    private final int selfPort;
    private final Path dataDir;
    private final Map<String, String> peerAddresses; // includes self
    private final int snapshotThreshold;
    private final int snapshotChunkSize;

    private RaftConfig(String selfId, int selfPort, Path dataDir, Map<String, String> peerAddresses,
                       int snapshotThreshold, int snapshotChunkSize) {
        this.selfId = selfId;
        this.selfPort = selfPort;
        this.dataDir = dataDir;
        this.peerAddresses = peerAddresses;
        this.snapshotThreshold = snapshotThreshold;
        this.snapshotChunkSize = snapshotChunkSize;
    }

    public static RaftConfig load(Path propertiesFile) throws IOException {
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(propertiesFile)) {
            props.load(in);
        }
        String selfId = require(props, "node.id");
        int port = Integer.parseInt(require(props, "node.port"));
        Path dataDir = Path.of(require(props, "data.dir"));
        String thresholdProp = props.getProperty("snapshot.threshold");
        int snapshotThreshold = thresholdProp == null ? DEFAULT_SNAPSHOT_THRESHOLD : Integer.parseInt(thresholdProp);
        String chunkSizeProp = props.getProperty("snapshot.chunk.size");
        int snapshotChunkSize = chunkSizeProp == null ? DEFAULT_SNAPSHOT_CHUNK_SIZE : Integer.parseInt(chunkSizeProp);

        Map<String, String> peers = new LinkedHashMap<>();
        for (String name : props.stringPropertyNames()) {
            if (name.startsWith("peer.")) {
                String peerId = name.substring("peer.".length());
                peers.put(peerId, props.getProperty(name));
            }
        }
        if (!peers.containsKey(selfId)) {
            throw new IllegalArgumentException("peer list must include self (" + selfId + ")");
        }
        return new RaftConfig(selfId, port, dataDir, peers, snapshotThreshold, snapshotChunkSize);
    }

    private static String require(Properties props, String key) {
        String v = props.getProperty(key);
        if (v == null) {
            throw new IllegalArgumentException("missing config key: " + key);
        }
        return v;
    }

    public String selfId() { return selfId; }
    public int selfPort() { return selfPort; }
    public Path dataDir() { return dataDir; }
    public Map<String, String> peerAddresses() { return peerAddresses; }

    public int clusterSize() { return peerAddresses.size(); }

    /** Number of servers needed to form a majority (§5.2). */
    public int majority() { return clusterSize() / 2 + 1; }

    /**
     * How many newly applied log entries a server lets accumulate since its
     * last snapshot before taking another one (§7). Defaults to
     * {@value #DEFAULT_SNAPSHOT_THRESHOLD} if {@code snapshot.threshold}
     * isn't set in the .properties file; the demo configs under
     * {@code config/} set a much smaller value so compaction is easy to
     * observe without sending hundreds of commands first.
     */
    public int snapshotThreshold() { return snapshotThreshold; }

    /**
     * Maximum size in bytes of each InstallSnapshot chunk (§7, Figure 13).
     * Defaults to 1 MB if {@code snapshot.chunk.size} isn't set.
     */
    public int snapshotChunkSize() { return snapshotChunkSize; }
}
