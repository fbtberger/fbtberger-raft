/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import io.grpc.Context;

import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import java.security.cert.X509Certificate;

/**
 * The node id an mTLS peer actually authenticated as, as opposed to the one it claims.
 *
 * <h2>The gap this closes</h2>
 * mTLS proves membership of the cluster PKI and nothing else: a certificate signed by the cluster
 * CA is accepted for <em>any</em> role. Every Raft RPC then carries the sender's identity as a
 * plain string in the payload — {@code AppendEntriesRequest.leaderId},
 * {@code RequestVoteRequest.candidateId} — and until now those strings never met the
 * authenticated peer. A single compromised key, a learner's for instance, was therefore enough to
 * open a legitimate connection and send {@code leaderId=kwatro1}: authentic transport, forged
 * sender. That is precisely the impersonation mTLS is deployed against, so proving PKI membership
 * alone bought less than it appeared to.
 *
 * <p>The binding is one comparison — the id in the message must equal the id in the certificate —
 * but it only exists if something carries the certificate's id to the place where the message is
 * read. That is what this class and {@link PeerIdentityInterceptor} do.
 *
 * <h2>Where the id lives in the certificate</h2>
 * The Common Name of the subject DN. A node certificate is issued as {@code CN=<nodeId>}, so
 * {@code kwatro1} presents {@code CN=kwatro1}. CN rather than a SAN entry because the value being
 * bound is a cluster-internal role name, not a hostname: nodes are reached by IP or by Compose
 * service name, neither of which equals the node id, and overloading the SAN would tie the
 * identity to the addressing — which is exactly what the July 2026 migration showed to be
 * fragile, when peer addresses moved and the identities did not.
 *
 * <p>Hostname verification is a separate, complementary check and is left to the TLS layer.
 */
public final class PeerIdentity {

    /**
     * The authenticated peer's node id for the current call, or {@code null} when the call did not
     * arrive over a connection with a client certificate.
     *
     * <p>Null is not the same as "unauthenticated and therefore fine": the enforcement decision
     * lives in {@link GrpcTransportServer}, which only requires a match when mTLS is configured.
     * A null here on an mTLS-enabled server means the interceptor could not read an identity, and
     * that is treated as a failure, not as an exemption.
     */
    public static final Context.Key<String> AUTHENTICATED_NODE_ID =
            Context.key("raft-authenticated-node-id");

    private PeerIdentity() { }

    /**
     * The node id carried in a certificate's subject CN, or {@code null} if there is none.
     *
     * <p>Parsed with {@link LdapName} rather than by splitting the string form of the DN: a DN is
     * a structured value and {@code getName()} escapes and reorders in ways that make substring
     * matching quietly wrong for names containing a comma or an equals sign. Attribute types are
     * case-insensitive per RFC 4514, hence the {@code equalsIgnoreCase}.
     *
     * @param certificate the peer's leaf certificate
     * @return the CN value, or null if the DN carries none
     */
    public static String fromCertificate(X509Certificate certificate) {
        if (certificate == null) return null;
        try {
            LdapName dn = new LdapName(certificate.getSubjectX500Principal().getName());
            for (Rdn rdn : dn.getRdns()) {
                if ("CN".equalsIgnoreCase(rdn.getType())) {
                    String value = String.valueOf(rdn.getValue()).trim();
                    return value.isEmpty() ? null : value;
                }
            }
            return null;
        } catch (javax.naming.InvalidNameException e) {
            // A certificate that got through the TLS handshake but whose DN will not parse is not
            // a case to guess about -- treat it as carrying no identity, which the caller rejects.
            return null;
        }
    }
}
