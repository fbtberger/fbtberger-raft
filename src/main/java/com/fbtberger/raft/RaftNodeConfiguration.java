/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.transport.GrpcTransportFactory;
import com.fbtberger.raft.transport.GrpcTransportServer;
import com.fbtberger.raft.transport.RaftTransportFactory;
import com.fbtberger.raft.transport.RaftTransportServer;
import com.fbtberger.raft.transport.TimeoutTransport;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import io.grpc.ServerBuilder;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

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
 *   <li>The transport {@link RaftTransportServer} is started as a bean so it's
 *       ready to accept peer RPCs before {@link RaftNode#start()} arms the
 *       election timer.</li>
 * </ul>
 *
 * <p>Note: {@link RaftNode#start()} is intentionally <em>not</em> called here —
 * it is called by {@link RaftServer} after the transport server is confirmed
 * running, maintaining the same explicit start sequence as before Spring was
 * introduced.
 */
@Configuration
public class RaftNodeConfiguration {

    @Bean
    public RaftConfig raftConfig(@Value("${raft.config.path}") String configPath)
            throws IOException {
        return RaftConfig.load(Path.of(configPath));
    }

    @Bean(destroyMethod = "close")
    public RaftStorage raftStorage(RaftConfig config) {
        return new BerkeleyDbStorage(config.dataDir().toFile());
    }

    @Bean
    @ConditionalOnMissingBean(StateMachine.class)
    public StateMachine stateMachine() {
        return new KeyValueStateMachine();
    }

    @Bean
    @ConditionalOnMissingBean(RaftTransportFactory.class)
    public RaftTransportFactory transportFactory(RaftConfig config) {
        RaftTransportFactory base = new GrpcTransportFactory(config.tlsConfig());
        return address -> new TimeoutTransport(base.connect(address), config.rpcTimeouts());
    }

    @Bean
    public PrometheusMeterRegistry meterRegistry() {
        return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    }

    @Bean(destroyMethod = "close")
    public JmxMeterRegistry jmxMeterRegistry() {
        return new JmxMeterRegistry(JmxConfig.DEFAULT, io.micrometer.core.instrument.Clock.SYSTEM);
    }

    @Bean
    public RaftMetrics raftMetrics(PrometheusMeterRegistry promRegistry,
                                    JmxMeterRegistry jmxRegistry,
                                    RaftConfig config) {
        CompositeMeterRegistry composite = new CompositeMeterRegistry();
        composite.add(promRegistry);
        composite.add(jmxRegistry);
        return new RaftMetrics(composite, config.selfId());
    }

    @Bean(destroyMethod = "shutdown")
    public RaftNode raftNode(RaftConfig config,
                              RaftStorage storage,
                              StateMachine stateMachine,
                              RaftTransportFactory transportFactory,
                              RaftMetrics metrics) {
        return new RaftNode(config, storage, stateMachine, transportFactory, metrics);
    }

    @Bean
    public RaftNodeMXBean raftNodeMBean(RaftNode raftNode, RaftStorage storage) {
        RaftNodeMBean mbean = new RaftNodeMBean(raftNode, storage);
        try {
            java.lang.management.ManagementFactory.getPlatformMBeanServer()
                    .registerMBean(mbean,
                            new javax.management.ObjectName("com.fbtberger.raft:type=RaftNode"));
        } catch (Exception e) {
            throw new RuntimeException("failed to register RaftNode MBean", e);
        }
        return mbean;
    }

    @Bean
    public RaftClientGrpcService raftClientGrpcService(RaftNode raftNode) {
        return new RaftClientGrpcService(raftNode);
    }

    @Bean(destroyMethod = "close")
    public RaftTransportServer raftTransportServer(RaftConfig config,
                                                    RaftNode raftNode,
                                                    RaftClientGrpcService clientService) throws Exception {
        GrpcTransportServer server;
        if (config.tlsConfig().enabled()) {
            server = new GrpcTransportServer(config.selfPort(), raftNode, config.tlsConfig());
            // Client-facing service added via separate gRPC server or same port
        } else {
            server = new GrpcTransportServer(
                    ServerBuilder.forPort(config.selfPort()).addService(clientService),
                    raftNode);
        }
        server.start();
        return server;
    }

    @Bean
    public HealthCheck healthCheck(RaftNode raftNode, RaftStorage storage) {
        return new HealthCheck(raftNode, storage);
    }

    @Bean(destroyMethod = "stop")
    public MetricsHttpServer metricsHttpServer(PrometheusMeterRegistry registry,
                                        RaftConfig config,
                                        HealthCheck healthCheck) throws Exception {
        if (config.metricsPort() <= 0) {
            return null;
        }
        HttpServer server;
        if (config.tlsConfig().enabled()) {
            // Change 78: same node identity as the peer transport (TlsConfig.buildJdkSslContext
            // reuses the same PEM cert/key), so the metrics/health endpoint speaks HTTPS too.
            HttpsServer httpsServer = HttpsServer.create(new InetSocketAddress(config.metricsPort()), 0);
            httpsServer.setHttpsConfigurator(new HttpsConfigurator(config.tlsConfig().buildJdkSslContext()));
            server = httpsServer;
        } else {
            server = HttpServer.create(new InetSocketAddress(config.metricsPort()), 0);
        }
        server.createContext("/metrics", exchange -> {
            String scrape = registry.scrape();
            byte[] body = scrape.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/health", exchange -> {
            HealthCheck.Status status = healthCheck.liveness();
            respondJson(exchange, status);
        });
        server.createContext("/ready", exchange -> {
            HealthCheck.Status status = healthCheck.readiness();
            respondJson(exchange, status);
        });
        server.start();
        return new MetricsHttpServer(server);
    }

    private static void respondJson(com.sun.net.httpserver.HttpExchange exchange,
                                     HealthCheck.Status status) throws IOException {
        byte[] body = status.toJson().getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status.ok() ? 200 : 503, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }
}
