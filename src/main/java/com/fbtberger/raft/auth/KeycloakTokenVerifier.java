/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.auth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.RemoteJWKSet;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Verifies Keycloak access tokens offline: signature against the realm's published keys,
 * then issuer, expiry and (if configured) audience.
 *
 * <p>Offline on purpose. Asking Keycloak about every request (token introspection) would
 * put an HTTP round trip to another system on the path of every command, and make the
 * cluster's write availability the product of its own and Keycloak's. With signature
 * verification Keycloak is needed when a key is first seen and again when the realm
 * rotates its keys; in between the node decides alone. The cost is the usual one: a token
 * revoked in Keycloak stays valid here until it expires, so keep access tokens short-lived.
 *
 * <p>Roles are read where Keycloak puts them: {@code realm_access.roles}, plus
 * {@code resource_access.<client>.roles} for the configured client id.
 */
public final class KeycloakTokenVerifier implements TokenVerifier {

    private final DefaultJWTProcessor<SecurityContext> processor;
    private final String clientId;

    public KeycloakTokenVerifier(ClientAuthConfig config) {
        this(config, remoteKeys(config.jwksUri()));
    }

    /** With the key source supplied, for tests that hold the keys themselves. */
    KeycloakTokenVerifier(ClientAuthConfig config, JWKSource<SecurityContext> keys) {
        this.clientId = config.clientId();
        this.processor = new DefaultJWTProcessor<>();
        // RS256 only, named here and not taken from the token's header: a verifier that
        // lets the token choose its algorithm can be talked into "none" or into checking
        // an HMAC with the public key as the secret.
        processor.setJWSKeySelector(new JWSVerificationKeySelector<>(JWSAlgorithm.RS256, keys));
        processor.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<>(
                config.audience(),
                new JWTClaimsSet.Builder().issuer(config.issuer()).build(),
                Set.of("sub", "exp")));
    }

    private static JWKSource<SecurityContext> remoteKeys(String jwksUri) {
        try {
            return new RemoteJWKSet<>(new URL(jwksUri));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("auth.keycloak.jwks.uri is not a URL: " + jwksUri, e);
        }
    }

    @Override
    public AuthenticatedClient verify(String bearerToken) throws InvalidTokenException {
        JWTClaimsSet claims;
        try {
            claims = processor.process(bearerToken, null);
        } catch (ParseException e) {
            throw new InvalidTokenException("token is not a JWT", e);
        } catch (BadJOSEException e) {
            throw new InvalidTokenException("token rejected: " + e.getMessage(), e);
        } catch (JOSEException e) {
            // Not a statement about the token: the keys could not be fetched or used.
            throw new InvalidTokenException("token could not be verified: " + e.getMessage(), e);
        }
        Set<String> roles = new LinkedHashSet<>();
        addRoles(roles, claims.getClaim("realm_access"));
        if (clientId != null && claims.getClaim("resource_access") instanceof Map<?, ?> resources) {
            addRoles(roles, resources.get(clientId));
        }
        return new AuthenticatedClient(claims.getSubject(), roles);
    }

    private static void addRoles(Set<String> into, Object access) {
        if (access instanceof Map<?, ?> map && map.get("roles") instanceof Collection<?> listed) {
            for (Object role : listed) {
                if (role instanceof String name) {
                    into.add(name);
                }
            }
        }
    }
}
