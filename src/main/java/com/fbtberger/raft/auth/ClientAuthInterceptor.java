/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.auth;

import com.fbtberger.raft.RaftMetrics;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decides, before a client RPC reaches {@code RaftClientGrpcService}, whether its caller
 * may make it.
 *
 * <p>The client channel carried no authorisation at all. Peers have been bound to their
 * certificates since the mTLS work, but {@code AddServer}, {@code RemoveServer} and
 * {@code TransferLeadership} were open to anyone who could reach the port, and a client
 * certificate, where one was required, only said that the caller was <em>some</em> holder
 * of a cluster certificate.
 *
 * <p>Each RPC needs exactly one role:
 *
 * <table>
 *   <caption>Required role per RPC</caption>
 *   <tr><td>{@code Query}</td><td>read role</td></tr>
 *   <tr><td>{@code Submit}</td><td>write role</td></tr>
 *   <tr><td>everything else</td><td>admin role</td></tr>
 * </table>
 *
 * "Everything else" is deliberate: an RPC added to the service later is admin-only until
 * someone decides otherwise, instead of open until someone notices. Roles do not imply
 * each other here; a hierarchy belongs in Keycloak, as composite roles, where the people
 * who grant roles can see it.
 *
 * <p>Wrap only the client service with this ({@code ServerInterceptors.intercept}). The
 * peer service shares the port and authenticates differently, by certificate.
 */
public final class ClientAuthInterceptor implements ServerInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(ClientAuthInterceptor.class);

    public static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    /** The verified caller of the current RPC, for handlers and logging. */
    public static final Context.Key<AuthenticatedClient> CLIENT = Context.key("raft-authenticated-client");

    private static final String BEARER = "Bearer ";

    private final TokenVerifier verifier;
    private final ClientAuthConfig config;
    private final RaftMetrics metrics;

    public ClientAuthInterceptor(TokenVerifier verifier, ClientAuthConfig config, RaftMetrics metrics) {
        this.verifier = verifier;
        this.config = config;
        this.metrics = metrics;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (!config.enabled()) {
            return next.startCall(call, headers);
        }
        String rpc = call.getMethodDescriptor().getBareMethodName();
        String header = headers.get(AUTHORIZATION);
        if (header == null || !header.regionMatches(true, 0, BEARER, 0, BEARER.length())) {
            return refuse(call, rpc, Status.UNAUTHENTICATED, "unauthenticated",
                    "a Keycloak bearer token is required");
        }
        AuthenticatedClient client;
        try {
            client = verifier.verify(header.substring(BEARER.length()).trim());
        } catch (TokenVerifier.InvalidTokenException e) {
            return refuse(call, rpc, Status.UNAUTHENTICATED, "unauthenticated", e.getMessage());
        }
        String required = requiredRole(rpc);
        if (!client.hasRole(required)) {
            return refuse(call, rpc, Status.PERMISSION_DENIED, "forbidden",
                    "role '" + required + "' is required for " + rpc + " (caller: " + client.subject() + ")");
        }
        metrics.clientAuthAccepted();
        return Contexts.interceptCall(Context.current().withValue(CLIENT, client), call, headers, next);
    }

    /** The one role an RPC needs. Anything not listed is an admin operation. */
    String requiredRole(String rpc) {
        if ("Query".equals(rpc)) {
            return config.readRole();
        }
        if ("Submit".equals(rpc)) {
            return config.writeRole();
        }
        return config.adminRole();
    }

    private <ReqT, RespT> ServerCall.Listener<ReqT> refuse(
            ServerCall<ReqT, RespT> call, String rpc, Status status, String reason, String detail) {
        // Logged on this side as well as returned: an operator looking at the node must be
        // able to see that it turned somebody away, and why, without asking the caller.
        LOG.warn("client RPC {} refused ({}): {}", rpc, status.getCode(), detail);
        metrics.clientAuthRejected(reason);
        call.close(status.withDescription(detail), new Metadata());
        return new ServerCall.Listener<>() { };
    }

    /** What the node tells its log at startup about who may use the client channel. */
    public static void logStartup(ClientAuthConfig config, boolean transportEncrypted) {
        if (!config.enabled()) {
            LOG.warn("Raft client channel: NO AUTHORISATION (auth.keycloak.enabled=false). Anyone who "
                    + "can reach this port may submit commands and reconfigure the cluster.");
            return;
        }
        LOG.info("Raft client channel: Keycloak tokens required, issuer {}, keys from {}, audience {}; "
                        + "roles: Query='{}', Submit='{}', admin operations='{}'",
                config.issuer(), config.jwksUri(), config.audience() == null ? "(any)" : config.audience(),
                config.readRole(), config.writeRole(), config.adminRole());
        if (!transportEncrypted) {
            LOG.warn("Raft client channel: tokens are required but tls.enabled=false, so every token "
                    + "crosses the network in the clear and can be replayed until it expires.");
        }
    }
}
