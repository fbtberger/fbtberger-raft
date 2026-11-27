/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.auth.KeycloakTokenVerifier;
import com.fbtberger.raft.auth.TokenVerifier;
import com.fbtberger.raft.client.RaftClient;
import com.fbtberger.raft.client.RaftClientException;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the Spring wiring puts in front of the client service on the node's port. The
 * interceptor's own behaviour is tested beside it; this is about whether a node built from
 * a properties file actually has it -- configuration being where "we have authorisation"
 * and "it is switched on" come apart.
 *
 * <p>The node is not started (see {@code RaftNodeConfigurationTest}), so a request that
 * gets past the interceptor is answered "not leader". That answer is the evidence: it can
 * only come from {@code RaftClientGrpcService}.
 */
class ClientChannelWiringTest {

    private static final byte[] SET = "SET k v".getBytes(StandardCharsets.UTF_8);

    @Test
    void byDefaultTheClientServiceAnswersWithoutAToken() throws Exception {
        int port = freePort();
        System.setProperty("raft.config.path", writeConfig("wire-open", port, new Properties()).toString());

        try (var ctx = new AnnotationConfigApplicationContext(RaftNodeConfiguration.class);
             RaftClient client = new RaftClient(Map.of("wire-open", "localhost:" + port))) {
            assertFalse(ctx.getBean(RaftConfig.class).clientAuthConfig().enabled());

            RaftClientException e = assertThrows(RaftClientException.class, () -> client.submit(SET));
            assertTrue(e.getMessage().contains("not leader"), e.getMessage());
        } finally {
            System.clearProperty("raft.config.path");
        }
    }

    @Test
    void withKeycloakConfiguredTheSameRequestIsRefusedBeforeItReachesTheNode() throws Exception {
        int port = freePort();
        Properties auth = new Properties();
        auth.setProperty("auth.keycloak.enabled", "true");
        auth.setProperty("auth.keycloak.issuer", "http://127.0.0.1:1/realms/raft");
        System.setProperty("raft.config.path", writeConfig("wire-auth", port, auth).toString());

        try (var ctx = new AnnotationConfigApplicationContext(RaftNodeConfiguration.class);
             RaftClient client = new RaftClient(Map.of("wire-auth", "localhost:" + port))) {
            assertInstanceOf(KeycloakTokenVerifier.class, ctx.getBean(TokenVerifier.class));

            RaftClientException e = assertThrows(RaftClientException.class, () -> client.submit(SET));
            assertTrue(e.getMessage().contains("not authenticated"), e.getMessage());
        } finally {
            System.clearProperty("raft.config.path");
        }
    }

    private static Path writeConfig(String id, int port, Properties extra) throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", id);
        props.setProperty("node.port", String.valueOf(port));
        props.setProperty("data.dir", Files.createTempDirectory("raft-wire-").toString());
        props.setProperty("peer." + id, "localhost:" + port);
        props.putAll(extra);
        Path tmp = Files.createTempFile("raft-wire-", ".properties");
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
