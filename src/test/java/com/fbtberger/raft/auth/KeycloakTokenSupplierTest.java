/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.auth;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeycloakTokenSupplierTest {

    private FakeKeycloak keycloak;
    private final AtomicLong clock = new AtomicLong(1_000_000);

    @BeforeEach
    void start() throws Exception {
        keycloak = new FakeKeycloak();
        keycloak.serviceAccount("raft-web", "s3cret", "raft-write");
    }

    @AfterEach
    void stop() {
        keycloak.close();
    }

    private KeycloakTokenSupplier supplier(String clientId, String secret) {
        return new KeycloakTokenSupplier(
                URI.create(keycloak.issuer() + "/protocol/openid-connect/token"), clientId, secret, clock::get);
    }

    @Test
    void theTokenItObtainsIsOneTheVerifierAccepts() throws Exception {
        String token = new KeycloakTokenSupplier(keycloak.issuer(), "raft-web", "s3cret").get();

        AuthenticatedClient client =
                new KeycloakTokenVerifier(ClientAuthConfig.keycloak(keycloak.issuer())).verify(token);
        assertEquals("service-account-raft-web", client.subject());
        assertTrue(client.hasRole("raft-write"));
    }

    @Test
    void aTokenIsReusedWhileItIsValid() {
        KeycloakTokenSupplier supplier = supplier("raft-web", "s3cret");

        String first = supplier.get();
        clock.addAndGet(60_000);
        String second = supplier.get();

        assertEquals(first, second);
        assertEquals(1, keycloak.tokenRequests(), "one request to Keycloak, not one per command");
    }

    @Test
    void aTokenIsReplacedBeforeItExpiresNotAfter() {
        keycloak.tokenLifetimeSeconds(300);
        KeycloakTokenSupplier supplier = supplier("raft-web", "s3cret");
        supplier.get();

        clock.addAndGet(280_000); // 20 s of validity left: inside the 30 s margin
        supplier.get();

        assertEquals(2, keycloak.tokenRequests());
    }

    @Test
    void aWrongSecretFailsWithKeycloaksReason() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> supplier("raft-web", "wrong").get());

        assertTrue(e.getMessage().contains("401"), e.getMessage());
        assertTrue(e.getMessage().contains("invalid_client"), e.getMessage());
    }

    @Test
    void anUnreachableKeycloakFailsWithItsAddress() {
        KeycloakTokenSupplier nowhere = new KeycloakTokenSupplier(
                URI.create("http://127.0.0.1:1/token"), "raft-web", "s3cret", clock::get);

        IllegalStateException e = assertThrows(IllegalStateException.class, nowhere::get);
        assertTrue(e.getMessage().contains("127.0.0.1:1"), e.getMessage());
    }
}
