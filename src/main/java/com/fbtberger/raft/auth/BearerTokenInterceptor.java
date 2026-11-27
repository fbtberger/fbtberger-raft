/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.auth;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.util.function.Supplier;

/**
 * Client side of {@link ClientAuthInterceptor}: puts the caller's current token on every
 * outgoing RPC. The supplier is asked per call, so a token refreshed in the background is
 * picked up without rebuilding the channel.
 */
public final class BearerTokenInterceptor implements ClientInterceptor {

    private final Supplier<String> token;

    public BearerTokenInterceptor(Supplier<String> token) {
        this.token = token;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                String current = token.get();
                if (current != null && !current.isEmpty()) {
                    headers.put(ClientAuthInterceptor.AUTHORIZATION, "Bearer " + current);
                }
                super.start(responseListener, headers);
            }
        };
    }
}
