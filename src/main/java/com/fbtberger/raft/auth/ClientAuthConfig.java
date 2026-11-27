/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.auth;

import java.util.Properties;

/**
 * Who may use the client channel, read from the node's {@code .properties} file.
 *
 * <pre>
 * auth.keycloak.enabled=true
 * auth.keycloak.issuer=https://keycloak.example/realms/raft   # required when enabled
 * auth.keycloak.jwks.uri=...        # default: issuer + /protocol/openid-connect/certs
 * auth.keycloak.audience=raft       # optional: required "aud" of the access token
 * auth.keycloak.client.id=raft      # optional: also accept this client's resource roles
 * auth.role.read=raft-read          # Query
 * auth.role.write=raft-write        # Submit
 * auth.role.admin=raft-admin        # membership changes and leadership transfer
 * </pre>
 *
 * <p>Off by default, because a cluster that upgrades must keep answering the clients it
 * has. Off means what it meant before this class existed: whoever can reach the port may
 * submit commands and reconfigure the cluster. The node says so at startup.
 */
public final class ClientAuthConfig {

    public static final String DEFAULT_ROLE_READ = "raft-read";
    public static final String DEFAULT_ROLE_WRITE = "raft-write";
    public static final String DEFAULT_ROLE_ADMIN = "raft-admin";

    private static final ClientAuthConfig DISABLED =
            new ClientAuthConfig(false, null, null, null, null,
                    DEFAULT_ROLE_READ, DEFAULT_ROLE_WRITE, DEFAULT_ROLE_ADMIN);

    private final boolean enabled;
    private final String issuer;
    private final String jwksUri;
    private final String audience;
    private final String clientId;
    private final String readRole;
    private final String writeRole;
    private final String adminRole;

    private ClientAuthConfig(boolean enabled, String issuer, String jwksUri, String audience, String clientId,
                             String readRole, String writeRole, String adminRole) {
        this.enabled = enabled;
        this.issuer = issuer;
        this.jwksUri = jwksUri;
        this.audience = audience;
        this.clientId = clientId;
        this.readRole = readRole;
        this.writeRole = writeRole;
        this.adminRole = adminRole;
    }

    public static ClientAuthConfig disabled() {
        return DISABLED;
    }

    /** Enabled, with the default role names and the realm's standard key endpoint. */
    public static ClientAuthConfig keycloak(String issuer) {
        return keycloak(issuer, null, null, null,
                DEFAULT_ROLE_READ, DEFAULT_ROLE_WRITE, DEFAULT_ROLE_ADMIN);
    }

    public static ClientAuthConfig keycloak(String issuer, String jwksUri, String audience, String clientId,
                                            String readRole, String writeRole, String adminRole) {
        if (issuer == null || issuer.isBlank()) {
            throw new IllegalArgumentException(
                    "auth.keycloak.issuer is required when auth.keycloak.enabled=true");
        }
        String trimmed = issuer.endsWith("/") ? issuer.substring(0, issuer.length() - 1) : issuer;
        String keys = jwksUri == null || jwksUri.isBlank()
                ? trimmed + "/protocol/openid-connect/certs" : jwksUri;
        return new ClientAuthConfig(true, trimmed, keys, blankToNull(audience), blankToNull(clientId),
                readRole, writeRole, adminRole);
    }

    public static ClientAuthConfig fromProperties(Properties props) {
        if (!Boolean.parseBoolean(props.getProperty("auth.keycloak.enabled", "false"))) {
            return DISABLED;
        }
        return keycloak(
                props.getProperty("auth.keycloak.issuer"),
                props.getProperty("auth.keycloak.jwks.uri"),
                props.getProperty("auth.keycloak.audience"),
                props.getProperty("auth.keycloak.client.id"),
                props.getProperty("auth.role.read", DEFAULT_ROLE_READ),
                props.getProperty("auth.role.write", DEFAULT_ROLE_WRITE),
                props.getProperty("auth.role.admin", DEFAULT_ROLE_ADMIN));
    }

    private static String blankToNull(String s) {
        return s == null || s.isBlank() ? null : s.trim();
    }

    public boolean enabled() { return enabled; }

    /** The realm URL every accepted token must name as its issuer. */
    public String issuer() { return issuer; }

    /** Where the realm publishes its signing keys. */
    public String jwksUri() { return jwksUri; }

    /** The audience a token must carry, or null if any audience is accepted. */
    public String audience() { return audience; }

    /** The Keycloak client whose resource roles count beside the realm roles, or null. */
    public String clientId() { return clientId; }

    public String readRole() { return readRole; }

    public String writeRole() { return writeRole; }

    public String adminRole() { return adminRole; }
}
