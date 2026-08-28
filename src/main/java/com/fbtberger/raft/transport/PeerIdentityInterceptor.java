/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;

/**
 * Lifts the client certificate's node id out of the TLS session and into the call {@link Context},
 * where {@link GrpcTransportServer.RaftServiceAdapter} can compare it against the sender id the
 * message declares.
 *
 * <p>An interceptor rather than a lookup inside each RPC method: the SSL session hangs off the
 * call's transport attributes, which the generated service base class does not expose. Doing it
 * once here also means the five RPC methods share one definition of "who is calling", instead of
 * four correct copies and one that was added later and forgot.
 *
 * <p>Only the leaf certificate is read. The chain above it has already been validated by the TLS
 * layer against the cluster CA; re-examining it here would be duplicating the trust decision, and
 * the identity question is about the peer itself.
 */
public final class PeerIdentityInterceptor implements ServerInterceptor {

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {

        String nodeId = null;
        SSLSession session = call.getAttributes().get(Grpc.TRANSPORT_ATTR_SSL_SESSION);
        if (session != null) {
            try {
                Certificate[] chain = session.getPeerCertificates();
                if (chain != null && chain.length > 0 && chain[0] instanceof X509Certificate leaf) {
                    nodeId = PeerIdentity.fromCertificate(leaf);
                }
            } catch (SSLPeerUnverifiedException e) {
                // No client certificate on this session. With ClientAuth.REQUIRE the handshake
                // would already have failed, so this is the plaintext / one-way-TLS case: leave
                // the identity unset and let the server decide whether that is acceptable.
                nodeId = null;
            }
        }

        Context context = Context.current().withValue(PeerIdentity.AUTHENTICATED_NODE_ID, nodeId);
        return Contexts.interceptCall(context, call, headers, next);
    }
}
