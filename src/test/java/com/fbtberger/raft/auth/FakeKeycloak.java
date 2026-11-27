/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.auth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The two Keycloak endpoints the Raft code talks to, on a local port: the realm's key set
 * and its token endpoint for the client credentials grant. Tokens are signed with a real
 * RSA key and laid out the way Keycloak lays them out ({@code realm_access.roles},
 * {@code resource_access.<client>.roles}), so the verifier under test does real work.
 *
 * <p>Not a mock of the verifier's collaborators: the production classes fetch keys and
 * tokens over HTTP from this exactly as they would from a realm.
 */
final class FakeKeycloak implements AutoCloseable {

    private record ServiceAccount(String secret, List<String> realmRoles) {}

    private final RSAKey key;
    private final HttpServer server;
    private final String issuer;
    private final Map<String, ServiceAccount> accounts = new ConcurrentHashMap<>();
    private final AtomicInteger tokenRequests = new AtomicInteger();
    private final AtomicInteger keyRequests = new AtomicInteger();
    private volatile long tokenLifetimeSeconds = 300;

    FakeKeycloak() throws Exception {
        this.key = newKey("realm-key-1");
        this.server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        this.issuer = "http://127.0.0.1:" + server.getAddress().getPort() + "/realms/raft";
        server.createContext("/realms/raft/protocol/openid-connect/certs", exchange -> {
            keyRequests.incrementAndGet();
            respond(exchange, 200, new JWKSet(key.toPublicJWK()).toString());
        });
        server.createContext("/realms/raft/protocol/openid-connect/token", this::issueToken);
        server.start();
    }

    static RSAKey newKey(String keyId) throws JOSEException {
        return new RSAKeyGenerator(2048).keyID(keyId).generate();
    }

    String issuer() {
        return issuer;
    }

    int tokenRequests() {
        return tokenRequests.get();
    }

    int keyRequests() {
        return keyRequests.get();
    }

    void tokenLifetimeSeconds(long seconds) {
        this.tokenLifetimeSeconds = seconds;
    }

    /** Registers a confidential client whose service account holds these realm roles. */
    void serviceAccount(String clientId, String secret, String... realmRoles) {
        accounts.put(clientId, new ServiceAccount(secret, List.of(realmRoles)));
    }

    /** A token as the realm would issue it to {@code subject} with these realm roles. */
    String token(String subject, String... realmRoles) throws JOSEException {
        return sign(key, claims(subject, List.of(realmRoles)).build());
    }

    JWTClaimsSet.Builder claims(String subject, List<String> realmRoles) {
        return new JWTClaimsSet.Builder()
                .issuer(issuer)
                .subject(subject)
                .issueTime(new Date())
                .expirationTime(new Date(System.currentTimeMillis() + tokenLifetimeSeconds * 1000))
                .claim("realm_access", Map.of("roles", realmRoles));
    }

    /** Signs with the realm's key. */
    String sign(JWTClaimsSet claims) throws JOSEException {
        return sign(key, claims);
    }

    static String sign(RSAKey signingKey, JWTClaimsSet claims) throws JOSEException {
        SignedJWT jwt = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(signingKey.getKeyID()).build(), claims);
        jwt.sign(new RSASSASigner(signingKey));
        return jwt.serialize();
    }

    private void issueToken(HttpExchange exchange) throws IOException {
        tokenRequests.incrementAndGet();
        Map<String, String> form = new HashMap<>();
        String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        for (String pair : body.split("&")) {
            String[] kv = pair.split("=", 2);
            form.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8),
                    kv.length > 1 ? URLDecoder.decode(kv[1], StandardCharsets.UTF_8) : "");
        }
        ServiceAccount account = accounts.get(form.get("client_id"));
        if (!"client_credentials".equals(form.get("grant_type"))
                || account == null || !account.secret().equals(form.get("client_secret"))) {
            respond(exchange, 401, "{\"error\":\"invalid_client\"}");
            return;
        }
        try {
            String token = sign(key, claims("service-account-" + form.get("client_id"),
                    account.realmRoles()).build());
            respond(exchange, 200, "{\"access_token\":\"" + token + "\",\"expires_in\":"
                    + tokenLifetimeSeconds + ",\"token_type\":\"Bearer\"}");
        } catch (JOSEException e) {
            respond(exchange, 500, "{\"error\":\"server_error\"}");
        }
    }

    private static void respond(HttpExchange exchange, int status, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    @Override
    public void close() {
        server.stop(0);
    }
}
