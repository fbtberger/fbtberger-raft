/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.auth;

import com.nimbusds.jose.util.JSONObjectUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Duration;
import java.util.Map;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Obtains an access token for a service account from Keycloak (OAuth 2.0 client
 * credentials grant) and hands out the same one until shortly before it expires.
 *
 * <p>For processes that talk to the cluster on their own behalf: a web tier forwarding
 * commands, an operator's script. Pass it to {@code RaftClient} as the token supplier. An
 * application that acts for a logged-in user passes that user's token instead and does not
 * need this class.
 *
 * <pre>
 * var token = new KeycloakTokenSupplier(
 *         "https://keycloak.example/realms/raft", "raft-web", clientSecret);
 * var client = new RaftClient(addresses, tlsConfig, token);
 * </pre>
 */
public final class KeycloakTokenSupplier implements Supplier<String> {

    /** A token this close to expiry is replaced, so one in flight does not expire on the way. */
    private static final long REFRESH_MARGIN_MILLIS = 30_000;

    private final URI tokenEndpoint;
    private final String form;
    private final HttpClient http;
    private final LongSupplier clockMillis;

    private String token;
    private long validUntilMillis;

    /**
     * @param issuer the realm URL, e.g. {@code https://keycloak.example/realms/raft}
     */
    public KeycloakTokenSupplier(String issuer, String clientId, String clientSecret) {
        this(URI.create(stripSlash(issuer) + "/protocol/openid-connect/token"),
                clientId, clientSecret, System::currentTimeMillis);
    }

    KeycloakTokenSupplier(URI tokenEndpoint, String clientId, String clientSecret, LongSupplier clockMillis) {
        this.tokenEndpoint = tokenEndpoint;
        this.form = "grant_type=client_credentials"
                + "&client_id=" + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
                + "&client_secret=" + URLEncoder.encode(clientSecret, StandardCharsets.UTF_8);
        this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        this.clockMillis = clockMillis;
    }

    private static String stripSlash(String url) {
        return url.endsWith("/") ? url.substring(0, url.length() - 1) : url;
    }

    @Override
    public synchronized String get() {
        long now = clockMillis.getAsLong();
        if (token == null || now >= validUntilMillis - REFRESH_MARGIN_MILLIS) {
            fetch(now);
        }
        return token;
    }

    private void fetch(long now) {
        HttpRequest request = HttpRequest.newBuilder(tokenEndpoint)
                .timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(HttpRequest.BodyPublishers.ofString(form))
                .build();
        HttpResponse<String> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new IllegalStateException("could not reach Keycloak at " + tokenEndpoint, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while asking Keycloak for a token", e);
        }
        if (response.statusCode() != 200) {
            // The body names the reason (invalid_client, unauthorized_client) and holds no secret.
            throw new IllegalStateException("Keycloak refused the token request: HTTP "
                    + response.statusCode() + " " + response.body());
        }
        try {
            Map<String, Object> body = JSONObjectUtils.parse(response.body());
            String accessToken = JSONObjectUtils.getString(body, "access_token");
            if (accessToken == null) {
                throw new IllegalStateException("Keycloak's token response has no access_token");
            }
            Object expiresIn = body.get("expires_in");
            long seconds = expiresIn instanceof Number n ? n.longValue() : 60;
            this.token = accessToken;
            this.validUntilMillis = now + seconds * 1000;
        } catch (ParseException e) {
            throw new IllegalStateException("Keycloak's token response is not JSON", e);
        }
    }
}
