package com.fbtberger.raft;

import com.fbtberger.raft.proto.RaftServiceGrpc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.function.Function;

/**
 * Spring {@link Configuration} used exclusively by {@link RaftNodeConfigurationTest}.
 *
 * <p>Mirrors the structure of the production {@link RaftNodeConfiguration} but
 * substitutes lightweight test doubles everywhere an external resource would
 * otherwise be required:
 * <ul>
 *   <li>{@link InMemoryStorage} instead of Berkeley DB — no on-disk environment needed.</li>
 *   <li>A no-op peer stub factory (returns {@code null}) — no network connections opened.</li>
 *   <li>No {@code grpcServer} bean — no port is bound, so the test can run on any machine
 *       regardless of which ports are free.</li>
 * </ul>
 *
 * <p>The configuration is intentionally kept as close to the production one as possible
 * so that the test exercises the real Spring wiring path (bean definitions, dependency
 * graph, lifecycle annotations) rather than bypassing it.
 */
@Configuration
class RaftNodeTestConfiguration {

    /**
     * Single-node cluster configuration written to a temp file so that
     * {@link RaftConfig#load(Path)} can be exercised end-to-end just as it
     * is in production, without having to expose an alternative constructor.
     */
    @Bean
    public RaftConfig raftConfig() throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", "test-node");
        props.setProperty("node.port", "19999");
        props.setProperty("data.dir", "/tmp/raft-spring-test-unused");
        props.setProperty("peer.test-node", "localhost:19999");
        props.setProperty("snapshot.threshold", "10");

        Path tmp = Files.createTempFile("raft-spring-test-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    /**
     * In-memory storage: no disk I/O, no Berkeley DB environment,
     * automatically cleaned up when the JVM exits.
     */
    @Bean
    public RaftStorage raftStorage() {
        return new InMemoryStorage();
    }

    /** The same state machine used in production. */
    @Bean
    public KeyValueStateMachine stateMachine() {
        return new KeyValueStateMachine();
    }

    /**
     * No-op stub factory: returns {@code null} for every address.
     * {@link RaftNode} skips replication to any peer whose stub is
     * {@code null}, so the node runs correctly in isolation without
     * attempting any network connections.
     */
    @Bean
    public Function<String, RaftServiceGrpc.RaftServiceFutureStub> peerStubFactory() {
        return address -> null;
    }

    /**
     * The real {@link RaftNode} — same class as in production — injected
     * with the test doubles above. {@code destroyMethod = "shutdown"}
     * ensures the scheduler is cancelled when the test context closes,
     * so no threads leak between test classes.
     */
    @Bean(destroyMethod = "shutdown")
    public RaftNode raftNode(RaftConfig config,
                              RaftStorage storage,
                              KeyValueStateMachine stateMachine,
                              Function<String, RaftServiceGrpc.RaftServiceFutureStub> peerStubFactory) {
        return new RaftNode(config, storage, stateMachine, peerStubFactory);
    }
}
