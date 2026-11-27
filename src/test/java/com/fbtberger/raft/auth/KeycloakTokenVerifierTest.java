/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.auth;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.jwt.SignedJWT;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What the node accepts as proof of identity. Each refusal below is a way of presenting a
 * token that looks right and is not; the keys are fetched over HTTP from a local realm
 * endpoint, the way they are fetched from Keycloak.
 */
class KeycloakTokenVerifierTest {

    private FakeKeycloak keycloak;
    private KeycloakTokenVerifier verifier;

    @BeforeEach
    void start() throws Exception {
        keycloak = new FakeKeycloak();
        verifier = new KeycloakTokenVerifier(ClientAuthConfig.keycloak(keycloak.issuer()));
    }

    @AfterEach
    void stop() {
        keycloak.close();
    }

    private void assertRefused(String token) {
        assertThrows(TokenVerifier.InvalidTokenException.class, () -> verifier.verify(token));
    }

    @Test
    void aTokenFromTheRealmYieldsItsSubjectAndRealmRoles() throws Exception {
        AuthenticatedClient client = verifier.verify(keycloak.token("alice", "raft-read", "raft-write"));

        assertEquals("alice", client.subject());
        assertEquals(Set.of("raft-read", "raft-write"), client.roles());
    }

    @Test
    void theKeysAreFetchedOnceNotPerToken() throws Exception {
        verifier.verify(keycloak.token("alice", "raft-read"));
        verifier.verify(keycloak.token("bob", "raft-read"));
        verifier.verify(keycloak.token("carol", "raft-read"));

        assertEquals(1, keycloak.keyRequests(),
                "verification must not put a Keycloak round trip on the path of every command");
    }

    @Test
    void aTokenSignedByAnotherKeyIsRefused() throws Exception {
        String forged = FakeKeycloak.sign(FakeKeycloak.newKey("realm-key-1"),
                keycloak.claims("mallory", List.of("raft-admin")).build());

        assertRefused(forged);
    }

    @Test
    void anExpiredTokenIsRefused() throws Exception {
        JWTClaimsSet expired = keycloak.claims("alice", List.of("raft-admin"))
                .expirationTime(new Date(System.currentTimeMillis() - 600_000)).build();

        assertRefused(keycloak.sign(expired));
    }

    @Test
    void aTokenWithoutAnExpiryIsRefused() throws Exception {
        JWTClaimsSet eternal = new JWTClaimsSet.Builder()
                .issuer(keycloak.issuer()).subject("alice")
                .claim("realm_access", Map.of("roles", List.of("raft-admin"))).build();

        assertRefused(keycloak.sign(eternal));
    }

    @Test
    void aTokenFromAnotherRealmIsRefused() throws Exception {
        JWTClaimsSet otherRealm = keycloak.claims("alice", List.of("raft-admin"))
                .issuer(keycloak.issuer().replace("/realms/raft", "/realms/other")).build();

        assertRefused(keycloak.sign(otherRealm));
    }

    @Test
    void anUnsignedTokenIsRefused() {
        String unsigned = new PlainJWT(keycloak.claims("mallory", List.of("raft-admin")).build()).serialize();

        assertRefused(unsigned);
    }

    @Test
    void aTokenThatPicksItsOwnAlgorithmIsRefused() throws Exception {
        // The classic confusion: HS256, with something public as the "secret".
        SignedJWT hmac = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256),
                keycloak.claims("mallory", List.of("raft-admin")).build());
        hmac.sign(new MACSigner("0123456789abcdef0123456789abcdef"));

        assertRefused(hmac.serialize());
    }

    @Test
    void garbageIsRefusedWithAReasonNotAStackTrace() {
        TokenVerifier.InvalidTokenException e = assertThrows(
                TokenVerifier.InvalidTokenException.class, () -> verifier.verify("not-a-token"));
        assertTrue(e.getMessage().contains("not a JWT"), e.getMessage());
    }

    @Test
    void aRequiredAudienceMustBePresent() throws Exception {
        KeycloakTokenVerifier strict = new KeycloakTokenVerifier(ClientAuthConfig.keycloak(
                keycloak.issuer(), null, "raft", null, "raft-read", "raft-write", "raft-admin"));
        String forRaft = keycloak.sign(keycloak.claims("alice", List.of("raft-read")).audience("raft").build());
        String forSomethingElse = keycloak.sign(
                keycloak.claims("alice", List.of("raft-read")).audience("account").build());

        assertEquals("alice", strict.verify(forRaft).subject());
        assertThrows(TokenVerifier.InvalidTokenException.class, () -> strict.verify(forSomethingElse),
                "a token issued for another application must not open this one");
    }

    @Test
    void theConfiguredClientsResourceRolesCountAndOtherClientsDoNot() throws Exception {
        KeycloakTokenVerifier withClient = new KeycloakTokenVerifier(ClientAuthConfig.keycloak(
                keycloak.issuer(), null, null, "raft-cli", "raft-read", "raft-write", "raft-admin"));
        JWTClaimsSet claims = keycloak.claims("alice", List.of("raft-read"))
                .claim("resource_access", Map.of(
                        "raft-cli", Map.of("roles", List.of("raft-write")),
                        "other-app", Map.of("roles", List.of("raft-admin"))))
                .build();

        AuthenticatedClient client = withClient.verify(keycloak.sign(claims));

        assertEquals(Set.of("raft-read", "raft-write"), client.roles());
    }

    @Test
    void anUnreachableRealmRefusesTheTokenInsteadOfAdmittingIt() {
        KeycloakTokenVerifier orphan = new KeycloakTokenVerifier(
                ClientAuthConfig.keycloak("http://127.0.0.1:1/realms/raft"));

        assertThrows(TokenVerifier.InvalidTokenException.class,
                () -> orphan.verify(keycloak.token("alice", "raft-admin")),
                "without keys nothing can be verified, and unverified must mean refused");
    }
}
