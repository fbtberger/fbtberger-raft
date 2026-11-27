/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.auth;

import java.util.Set;

/** A caller whose token was verified: who it is and which roles the realm gave it. */
public record AuthenticatedClient(String subject, Set<String> roles) {

    public AuthenticatedClient {
        roles = Set.copyOf(roles);
    }

    public boolean hasRole(String role) {
        return roles.contains(role);
    }
}
