/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.auth;

import com.fbtberger.raft.InMemoryStorage;
import com.fbtberger.raft.KeyValueStateMachine;
import com.fbtberger.raft.RaftClientGrpcService;
import com.fbtberger.raft.RaftConfig;
import com.fbtberger.raft.RaftMetrics;
import com.fbtberger.raft.RaftNode;
import com.fbtberger.raft.ServerRole;
import com.fbtberger.raft.client.RaftClient;
import com.fbtberger.raft.client.RaftClientException;
import com.fbtberger.raft.transport.GrpcTransportServer;
import com.fbtberger.raft.transport.TlsConfig;
import io.grpc.ServerInterceptors;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The client channel with authorisation switched on, end to end: a real node on a real
 * port, the production interceptor and verifier in front of the client service, a
 * {@link RaftClient} on the other side, and a local realm endpoint issuing the tokens.
 *
 * <p>The claim under test is the matrix -- which role opens which RPC -- and that a
 * refusal reaches the caller as a refusal and not as an unreachable node.
 */
class KeycloakProtectedClientChannelTest {

    private static final byte[] SET = "SET k v".getBytes(StandardCharsets.UTF_8);
    private static final byte[] GET = "GET k".getBytes(StandardCharsets.UTF_8);

    private FakeKeycloak keycloak;
    private RaftNode node;
    private GrpcTransportServer server;
    private RaftMetrics metrics;
    private Map<String, String> cluster;

    @BeforeEach
    void start() throws Exception {
        keycloak = new FakeKeycloak();
        ClientAuthConfig auth = ClientAuthConfig.keycloak(keycloak.issuer());
        metrics = new RaftMetrics(new SimpleMeterRegistry(), "auth1");

        RaftConfig config = RaftConfig.of("auth1", 0, Path.of("unused"), Map.of("auth1", "localhost:0"));
        node = new RaftNode(config, new InMemoryStorage(), new KeyValueStateMachine(), address -> null, metrics);
        ClientAuthInterceptor interceptor =
                new ClientAuthInterceptor(new KeycloakTokenVerifier(auth), auth, metrics);
        server = new GrpcTransportServer(0, node, TlsConfig.disabled(),
                List.of(ServerInterceptors.intercept(new RaftClientGrpcService(node), interceptor)));
        server.start();
        cluster = Map.of("auth1", "localhost:" + server.grpcServer().getPort());

        node.start();
        long deadline = System.currentTimeMillis() + 5_000;
        while (node.role() != ServerRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(ServerRole.LEADER, node.role());
    }

    @AfterEach
    void stop() {
        node.shutdown();
        server.close();
        keycloak.close();
    }

    private RaftClient clientWith(String... realmRoles) throws Exception {
        String token = keycloak.token("alice", realmRoles);
        return new RaftClient(cluster, TlsConfig.disabled(), () -> token);
    }

    private double rejected(String reason) {
        return metrics.registry().get("raft.client.auth.rejected").tag("reason", reason).counter().count();
    }

    @Test
    void aCallerWithoutATokenIsTurnedAwayAndToldWhy() {
        try (RaftClient anonymous = new RaftClient(cluster)) {
            RaftClientException e = assertThrows(RaftClientException.class, () -> anonymous.submit(SET));

            assertTrue(e.getMessage().contains("not authenticated"), e.getMessage());
            assertTrue(e.getMessage().contains("bearer token is required"), e.getMessage());
        }
        assertEquals(1, rejected("unauthenticated"));
    }

    @Test
    void theWriteRoleOpensSubmitAndTheReadRoleOpensQuery() throws Exception {
        try (RaftClient client = clientWith("raft-read", "raft-write")) {
            assertArrayEquals("OK".getBytes(StandardCharsets.UTF_8), client.submit(SET));
            assertArrayEquals("v".getBytes(StandardCharsets.UTF_8), client.query(GET));
        }
        assertEquals(2, metrics.registry().get("raft.client.auth.accepted").counter().count());
    }

    @Test
    void aReaderCannotWrite() throws Exception {
        try (RaftClient reader = clientWith("raft-read")) {
            RaftClientException e = assertThrows(RaftClientException.class, () -> reader.submit(SET));

            assertTrue(e.getMessage().contains("not authorised"), e.getMessage());
            assertTrue(e.getMessage().contains("raft-write"), "the refusal names the missing role: " + e.getMessage());
        }
        assertEquals(1, rejected("forbidden"));
    }

    @Test
    void aWriterCannotReconfigureTheClusterOrMoveTheLeader() throws Exception {
        try (RaftClient writer = clientWith("raft-read", "raft-write")) {
            for (Attempt attempt : List.<Attempt>of(
                    () -> writer.addServer("n9", "localhost:1"),
                    () -> writer.removeServer("auth1"),
                    () -> writer.addLearner("n9", "localhost:1"),
                    () -> writer.promoteLearner("n9"),
                    () -> writer.removeLearner("n9"),
                    () -> writer.transferLeadership("auth1"))) {
                RaftClientException e = assertThrows(RaftClientException.class, attempt::run);
                assertTrue(e.getMessage().contains("raft-admin"), e.getMessage());
            }
        }
        assertEquals(Map.of("auth1", "localhost:0"), node.currentConfiguration(),
                "none of the refused calls may have reached the node");
    }

    @Test
    void theAdminRoleOpensMembershipChangesAndNothingElse() throws Exception {
        try (RaftClient admin = clientWith("raft-admin")) {
            admin.addLearner("n9", "localhost:1");
            assertTrue(node.currentLearners().containsKey("n9"));

            assertThrows(RaftClientException.class, () -> admin.submit(SET),
                    "roles do not imply each other; a hierarchy is Keycloak's to define");
        }
    }

    @Test
    void anExpiredTokenIsRefusedAsUnauthenticated() throws Exception {
        String expired = keycloak.sign(keycloak.claims("alice", List.of("raft-write"))
                .expirationTime(new java.util.Date(System.currentTimeMillis() - 600_000)).build());
        try (RaftClient client = new RaftClient(cluster, TlsConfig.disabled(), () -> expired)) {
            RaftClientException e = assertThrows(RaftClientException.class, () -> client.submit(SET));
            assertTrue(e.getMessage().contains("not authenticated"), e.getMessage());
        }
    }

    @Test
    void aServiceAccountTokenFromKeycloakIsPickedUpPerCall() throws Exception {
        keycloak.serviceAccount("raft-web", "s3cret", "raft-write", "raft-read");
        Supplier<String> token = new KeycloakTokenSupplier(keycloak.issuer(), "raft-web", "s3cret");

        try (RaftClient client = new RaftClient(cluster, TlsConfig.disabled(), token)) {
            client.submit(SET);
            assertArrayEquals("v".getBytes(StandardCharsets.UTF_8), client.query(GET));
        }
        assertEquals(1, keycloak.tokenRequests());
    }

    @Test
    void withAuthorisationOffEveryCallPassesAsBefore() throws Exception {
        ClientAuthInterceptor off = new ClientAuthInterceptor(
                token -> { throw new AssertionError("must not be asked"); },
                ClientAuthConfig.disabled(), metrics);
        GrpcTransportServer open = new GrpcTransportServer(0, node, TlsConfig.disabled(),
                List.of(ServerInterceptors.intercept(new RaftClientGrpcService(node), off)));
        open.start();
        try (RaftClient client = new RaftClient(Map.of("auth1", "localhost:" + open.grpcServer().getPort()))) {
            assertArrayEquals("OK".getBytes(StandardCharsets.UTF_8), client.submit(SET));
        } finally {
            open.close();
        }
    }

    private interface Attempt {
        void run() throws Exception;
    }
}
