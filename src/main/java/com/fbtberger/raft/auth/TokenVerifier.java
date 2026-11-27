/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.auth;

/**
 * Turns a bearer token into a caller, or refuses it.
 *
 * <p>An interface so that the decision "is this token good" is injected into
 * {@link ClientAuthInterceptor} rather than built inside it: the node wires
 * {@link KeycloakTokenVerifier}, a deployment with another identity provider supplies its
 * own bean, and the interceptor's tests need no key server.
 */
public interface TokenVerifier {

    /**
     * @param bearerToken the token as sent, without the {@code Bearer } prefix
     * @throws InvalidTokenException if the token is malformed, not signed by the realm,
     *                               expired, or issued for someone else
     */
    AuthenticatedClient verify(String bearerToken) throws InvalidTokenException;

    /** Why a token was refused. The message is safe to return to the caller. */
    final class InvalidTokenException extends Exception {
        public InvalidTokenException(String message) {
            super(message);
        }

        public InvalidTokenException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
