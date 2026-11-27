/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.client.RaftClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The files under {@code deploy/compose} against the code that reads them.
 *
 * <p>A compose setup is configuration nobody compiles. What can go stale without anyone
 * noticing is checked here: that each node's file loads, that the three files describe the
 * same cluster, and that the names in them agree with docker-compose.yml, the certificate
 * script and the Keycloak realm.
 */
class ComposeDeploymentTest {

    private static final Path COMPOSE = Path.of("deploy", "compose");

    @Test
    void everyNodeFileLoadsAndTheThreeDescribeTheSameCluster() throws Exception {
        Map<String, String> peers = null;
        for (int n = 1; n <= 3; n++) {
            RaftConfig config = RaftConfig.load(COMPOSE.resolve("config/node" + n + ".properties"));

            assertEquals("node" + n, config.selfId());
            assertEquals(9090 + n, config.selfPort());
            assertEquals("raft-" + n + ":" + (9090 + n), config.peerAddresses().get("node" + n));
            assertEquals(10090 + n, config.metricsPort(), "the health check in docker-compose.yml asks this port");
            if (peers == null) peers = config.peerAddresses();
            assertEquals(peers, config.peerAddresses(), "node" + n + " disagrees about who is in the cluster");

            assertTrue(config.tlsConfig().enabled());
            assertTrue(config.tlsConfig().mtlsEnabled());
            // Under mTLS the sender id is checked against the certificate CN, and certs.sh
            // names the files after the CN it issues.
            assertEquals("node" + n + ".crt", config.tlsConfig().certFile().getName());
            assertEquals("node" + n + ".key", config.tlsConfig().keyFile().getName());
            assertTrue(config.tlsConfig().reloadIntervalSeconds() > 0, "a renewal must be picked up");

            assertTrue(config.clientAuthConfig().enabled());
            assertFalse(config.isWitness(), "three data nodes unless someone uncomments the line");
        }
    }

    @Test
    void theIssuerTheNodesExpectIsTheOneKeycloakIssuesUnder() throws Exception {
        String compose = Files.readString(COMPOSE.resolve("docker-compose.yml"));
        String realm = Files.readString(COMPOSE.resolve("keycloak/raft-realm.json"));
        RaftConfig node = RaftConfig.load(COMPOSE.resolve("config/node1.properties"));
        RaftConfig client = RaftConfig.load(COMPOSE.resolve("config/client.properties"));

        assertTrue(compose.contains("KC_HOSTNAME: http://localhost:8080"));
        assertEquals("http://localhost:8080/realms/raft", node.clientAuthConfig().issuer());
        assertEquals(node.clientAuthConfig().issuer(), client.clientAuthConfig().issuer(),
                "a token fetched by the client must be one the nodes accept");
        assertTrue(realm.contains("\"realm\": \"raft\""));
        for (String role : List.of(node.clientAuthConfig().readRole(), node.clientAuthConfig().writeRole(),
                node.clientAuthConfig().adminRole())) {
            assertTrue(realm.contains("\"name\": \"" + role + "\""), "the realm lacks the role " + role);
        }
    }

    @Test
    void composeStartsEveryNodeWithItsOwnFileItsPortsAndAHealthCheck() throws Exception {
        String compose = Files.readString(COMPOSE.resolve("docker-compose.yml"));

        for (int n = 1; n <= 3; n++) {
            assertTrue(compose.contains("RAFT_CONFIG_PATH: /etc/raft/config/node" + n + ".properties"));
            assertTrue(compose.contains("\"" + (9090 + n) + ":" + (9090 + n) + "\""));
            assertTrue(compose.contains("https://localhost:" + (10090 + n) + "/ready"),
                    "node " + n + " has no health check on its readiness endpoint");
        }
        assertTrue(compose.contains("-Draft.cli.enabled=false"),
                "without it a node stops as soon as the container's stdin ends");
        assertTrue(Files.readString(Path.of("config", "Dockerfile")).contains("${JAVA_OPTS}"));
    }

    @Test
    void theClientFileReachesTheNodesThroughThePublishedPorts() throws Exception {
        RaftConfig client = RaftConfig.load(COMPOSE.resolve("config/client.properties"));

        assertEquals(Map.of("node1", "localhost:9091", "node2", "localhost:9092", "node3", "localhost:9093"),
                client.peerAddresses());
        assertEquals("client.crt", client.tlsConfig().certFile().getName());
    }

    @Test
    void theDemoClientRefusesToStartWithoutCredentialsForAClusterThatWantsTokens(@TempDir Path dir)
            throws Exception {
        RaftConfig client = RaftConfig.load(COMPOSE.resolve("config/client.properties"));
        Method connect = Class.forName("com.fbtberger.raft.client.RaftClientDemo")
                .getDeclaredMethod("connect", RaftConfig.class, Map.class);
        connect.setAccessible(true);

        Exception refusal = assertThrows(Exception.class, () -> connect.invoke(null, client, Map.of()));
        assertTrue(refusal.getCause().getMessage().contains("RAFT_KEYCLOAK_CLIENT_ID"),
                refusal.getCause().getMessage());

        // A plaintext cluster without tokens needs neither.
        Properties plain = new Properties();
        plain.setProperty("node.id", "n1");
        plain.setProperty("node.port", "9091");
        plain.setProperty("data.dir", dir.toString());
        plain.setProperty("peer.n1", "localhost:9091");
        Path file = dir.resolve("plain.properties");
        try (var out = Files.newOutputStream(file)) {
            plain.store(out, null);
        }
        try (RaftClient open = (RaftClient) connect.invoke(null, RaftConfig.load(file), Map.of())) {
            assertNotNull(open);
        }
    }
}
