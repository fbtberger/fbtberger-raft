/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.auth;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClientAuthConfigTest {

    @Test
    void authorisationIsOffUnlessAskedFor() {
        assertFalse(ClientAuthConfig.fromProperties(new Properties()).enabled(),
                "an upgraded node must keep answering the clients it already has");
    }

    @Test
    void theKeyEndpointDefaultsToTheRealmsStandardOne() {
        Properties props = new Properties();
        props.setProperty("auth.keycloak.enabled", "true");
        props.setProperty("auth.keycloak.issuer", "https://kc.example/realms/raft/");

        ClientAuthConfig config = ClientAuthConfig.fromProperties(props);

        assertTrue(config.enabled());
        assertEquals("https://kc.example/realms/raft", config.issuer(),
                "a trailing slash must not make every token's issuer mismatch");
        assertEquals("https://kc.example/realms/raft/protocol/openid-connect/certs", config.jwksUri());
        assertNull(config.audience());
        assertEquals("raft-read", config.readRole());
        assertEquals("raft-write", config.writeRole());
        assertEquals("raft-admin", config.adminRole());
    }

    @Test
    void everySettingCanBeOverridden() {
        Properties props = new Properties();
        props.setProperty("auth.keycloak.enabled", "true");
        props.setProperty("auth.keycloak.issuer", "https://kc.example/realms/raft");
        props.setProperty("auth.keycloak.jwks.uri", "https://internal/keys");
        props.setProperty("auth.keycloak.audience", "raft");
        props.setProperty("auth.keycloak.client.id", "raft-cli");
        props.setProperty("auth.role.read", "r");
        props.setProperty("auth.role.write", "w");
        props.setProperty("auth.role.admin", "a");

        ClientAuthConfig config = ClientAuthConfig.fromProperties(props);

        assertEquals("https://internal/keys", config.jwksUri());
        assertEquals("raft", config.audience());
        assertEquals("raft-cli", config.clientId());
        assertEquals("r", config.readRole());
        assertEquals("w", config.writeRole());
        assertEquals("a", config.adminRole());
    }

    @Test
    void enablingWithoutAnIssuerFailsAtStartupNotAtTheFirstRequest() {
        Properties props = new Properties();
        props.setProperty("auth.keycloak.enabled", "true");

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ClientAuthConfig.fromProperties(props));
        assertTrue(e.getMessage().contains("auth.keycloak.issuer"));
    }
}
