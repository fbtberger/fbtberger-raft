package com.fbtberger.raft;

import com.fbtberger.raft.proto.RaftServiceGrpc;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;

/**
 * Spring IoC configuration for a single Raft node.
 *
 * <p>All wiring is done here via {@link Bean @Bean} factory methods, keeping
 * every Raft class itself free of Spring annotations (no {@code @Component},
 * no {@code @Autowired} in production code). This follows the pure Inversion-
 * of-Control style: dependencies are pushed in from the outside rather than
 * pulled by the objects that need them.
 *
 * <p>Bootstrap: set the system property {@code raft.config.path} to the path
 * of a node's {@code .properties} file, then create an
 * {@link org.springframework.context.annotation.AnnotationConfigApplicationContext}
 * with this class. {@link RaftServer} does exactly that.
 *
 * <h2>Bean lifecycle</h2>
 * <ul>
 *   <li>{@link RaftConfig} is loaded first; every other bean depends on it.</li>
 *   <li>{@link RaftStorage} (backed by Berkeley DB) is opened next and registered
 *       with {@code destroyMethod = "close"} so Spring flushes and closes the
 *       database when the context is shut down.</li>
 *   <li>{@link RaftNode} is constructed after storage and state machine are ready,
 *       and is shut down ({@code destroyMethod = "shutdown"}) before storage is
 *       closed, ensuring no in-flight writes are lost.</li>
 *   <li>The gRPC {@link Server} is started as a bean so it's ready to accept peer
 *       RPCs before {@link RaftNode#start()} arms the election timer. It is shut
 *       down ({@code destroyMethod = "shutdown"}) first in teardown so no new
 *       RPCs can arrive while Raft is cleaning up.</li>
 * </ul>
 *
 * <p>Note: {@link RaftNode#start()} is intentionally <em>not</em> called here —
 * it is called by {@link RaftServer} after the gRPC server is confirmed running,
 * maintaining the same explicit start sequence as before Spring was introduced.
 */
@Configuration
public class RaftNodeConfiguration {

    /**
     * Loads the node's configuration from the file path provided via the
     * {@code raft.config.path} system property (set by {@link RaftServer}
     * before creating the application context).
     */
    @Bean
    public RaftConfig raftConfig(@Value("${raft.config.path}") String configPath)
            throws IOException {
        return RaftConfig.load(Path.of(configPath));
    }

    /**
     * Opens (or creates) the Berkeley DB environment in the directory
     * specified by {@link RaftConfig#dataDir()}.
     *
     * <p>{@code destroyMethod = "close"} tells Spring to call
     * {@link RaftStorage#close()} when the context shuts down, ensuring
     * all durable writes are flushed before the JVM exits.
     */
    @Bean(destroyMethod = "close")
    public RaftStorage raftStorage(RaftConfig config) {
        return new BerkeleyDbStorage(config.dataDir().toFile());
    }

    /**
     * Default state machine (key-value store). A production deployment
     * overrides this by providing its own {@link StateMachine} bean.
     */
    @Bean
    @ConditionalOnMissingBean(StateMachine.class)
    public StateMachine stateMachine() {
        return new KeyValueStateMachine();
    }

    /**
     * Factory function that turns a peer address (e.g. {@code "host:9092"})
     * into a non-blocking gRPC future stub. Injecting this as a bean
     * instead of hard-coding it inside {@link RaftNode} keeps the node
     * testable: tests substitute a stub factory that routes RPCs in-process
     * (or just returns {@code null} for a no-peer unit test) without any
     * production code change.
     */
    @Bean
    public Function<String, RaftServiceGrpc.RaftServiceFutureStub> peerStubFactory() {
        return address -> RaftServiceGrpc.newFutureStub(
                ManagedChannelBuilder.forTarget(address).usePlaintext().build());
    }

    /**
     * The core Raft state machine. All four collaborators are injected by
     * Spring — no object creates its own dependencies.
     *
     * <p>{@code destroyMethod = "shutdown"} cancels the election / heartbeat
     * scheduler and closes all open peer stubs when the context is shut down.
     */
    @Bean(destroyMethod = "shutdown")
    public RaftNode raftNode(RaftConfig config,
                              RaftStorage storage,
                              StateMachine stateMachine,
                              Function<String, RaftServiceGrpc.RaftServiceFutureStub> peerStubFactory) {
        return new RaftNode(config, storage, stateMachine, peerStubFactory);
    }

    /**
     * gRPC service adapter for server-to-server Raft RPCs
     * (RequestVote, AppendEntries, InstallSnapshot).
     */
    @Bean
    public RaftGrpcService raftGrpcService(RaftNode raftNode) {
        return new RaftGrpcService(raftNode);
    }

    /**
     * gRPC service adapter for client-facing RPCs
     * (Submit, AddServer, RemoveServer).
     */
    @Bean
    public RaftClientGrpcService raftClientGrpcService(RaftNode raftNode) {
        return new RaftClientGrpcService(raftNode);
    }

    /**
     * Starts the gRPC server on the port declared in {@link RaftConfig}.
     * Starting it here, before {@link RaftNode#start()} arms the election
     * timer, ensures that peer RPCs can be received from the moment the
     * node is visible on the network.
     *
     * <p>{@code destroyMethod = "shutdown"} stops the server gracefully
     * when the context is closed, preventing new RPCs from arriving during
     * Raft teardown.
     */
    @Bean(destroyMethod = "shutdown")
    public Server grpcServer(RaftConfig config,
                              RaftGrpcService raftGrpcService,
                              RaftClientGrpcService raftClientGrpcService) throws IOException {
        return ServerBuilder.forPort(config.selfPort())
                .addService(raftGrpcService)
                .addService(raftClientGrpcService)
                .build()
                .start();
    }
}
