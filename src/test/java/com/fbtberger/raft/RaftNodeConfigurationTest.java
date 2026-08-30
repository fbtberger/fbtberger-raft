/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.transport.RaftTransportFactory;
import com.fbtberger.raft.transport.RaftTransportServer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The wiring that decides whether a node comes up at all.
 *
 * <p>This class is how {@code RaftServer.main} builds everything: it reads the properties path
 * out of a system property, loads the config, opens storage, picks a transport, assembles the
 * metrics and hands all of it to a {@link RaftNode}. It was at 38 % coverage, and the parts that
 * were dark are the parts a deployment gets wrong — a path that does not exist, a storage type
 * nobody recognises, a port already taken.
 *
 * <p>Tested by booting the context for real rather than by calling the {@code @Bean} methods by
 * hand. Calling them directly would prove that a method returns what its body says; booting
 * proves that Spring can satisfy every dependency in it, which is the thing that actually fails.
 *
 * <p><b>The node is deliberately not started.</b> That is this configuration's own contract —
 * see its class comment: {@code RaftNode.start()} belongs to {@code RaftServer}, after the
 * transport server is confirmed listening. A test that started it would be testing a sequence
 * this class explicitly does not own.
 */
class RaftNodeConfigurationTest {

    @Test
    @DisplayName("The context assembles a whole node from a properties file")
    void bootsAWholeNode() throws Exception {
        Path cfg = writeConfig("cfg-node", freePort());
        System.setProperty("raft.config.path", cfg.toString());

        try (var ctx = new AnnotationConfigApplicationContext(RaftNodeConfiguration.class)) {
            RaftConfig config = ctx.getBean(RaftConfig.class);
            assertEquals("cfg-node", config.selfId());

            assertNotNull(ctx.getBean(RaftNode.class));
            assertNotNull(ctx.getBean(KeyValueStateMachine.class));
            assertNotNull(ctx.getBean(RaftStorage.class));
            assertNotNull(ctx.getBean(RaftMetrics.class));
            assertNotNull(ctx.getBean(RaftTransportServer.class));

            // Singletons, not a fresh object per injection point: a second RaftNode would be a
            // second state machine writing the same log directory.
            assertSame(ctx.getBean(RaftNode.class), ctx.getBean(RaftNode.class));
        } finally {
            System.clearProperty("raft.config.path");
        }
    }

    /**
     * The transport every peer call goes through is wrapped in a timeout. Without the wrapper a
     * peer that accepts a connection and then says nothing blocks a replication thread for good,
     * which is the failure that does not look like a failure.
     */
    @Test
    @DisplayName("Peer transports are wrapped so a silent peer cannot block for ever")
    void peerTransportsCarryATimeout() throws Exception {
        Path cfg = writeConfig("cfg-timeout", freePort());
        System.setProperty("raft.config.path", cfg.toString());

        try (var ctx = new AnnotationConfigApplicationContext(RaftNodeConfiguration.class)) {
            RaftTransportFactory factory = ctx.getBean(RaftTransportFactory.class);
            assertNotNull(factory);
            var transport = factory.connect("localhost:1");   // never dialled until used
            assertTrue(transport.getClass().getSimpleName().contains("Timeout"),
                    "expected a timeout wrapper, got " + transport.getClass().getName());
        } finally {
            System.clearProperty("raft.config.path");
        }
    }

    /**
     * A path that is not there must fail while the context is being built, loudly. The
     * alternative — a node that starts and then cannot say what it is — is what makes a bad
     * deployment look like a network problem.
     */
    @Test
    @DisplayName("A configuration file that does not exist stops the context, not the cluster")
    void missingConfigFileFailsFast() {
        System.setProperty("raft.config.path", "/definitely/not/here/node.properties");
        try {
            assertThrows(Exception.class,
                    () -> new AnnotationConfigApplicationContext(RaftNodeConfiguration.class).close());
        } finally {
            System.clearProperty("raft.config.path");
        }
    }

    // ---- helpers ------------------------------------------------------------

    private static Path writeConfig(String id, int port) throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", id);
        props.setProperty("node.port", String.valueOf(port));
        props.setProperty("data.dir", Files.createTempDirectory("raft-cfg-").toString());
        props.setProperty("peer." + id, "localhost:" + port);
        props.setProperty("snapshot.threshold", "100");
        Path tmp = Files.createTempFile("raft-cfg-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return tmp;
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
