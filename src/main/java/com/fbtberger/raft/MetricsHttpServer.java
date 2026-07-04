/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.sun.net.httpserver.HttpServer;

/**
 * Thin wrapper around the JDK's {@link HttpServer}, existing solely to give Spring a no-arg
 * destroy method to call.
 *
 * <p>{@code HttpServer}'s own stop method is {@code stop(int delay)} — it takes an {@code int}
 * grace-period parameter, with no no-arg overload. Spring's {@code @Bean(destroyMethod = ...)}
 * mechanism can only invoke a no-arg destroy method; declaring a bean of type {@code HttpServer}
 * directly with {@code destroyMethod = "stop"} therefore fails bean definition validation at
 * context startup:
 *
 * <pre>
 * BeanDefinitionValidationException: Method 'stop' of bean 'metricsHttpServer' has a
 * non-boolean parameter — not supported as destroy method
 * </pre>
 *
 * <p>{@link RaftNodeConfiguration#metricsHttpServer} returns this wrapper instead of the raw
 * {@code HttpServer}, keeping the same {@code destroyMethod = "stop"} bean declaration — only
 * now it resolves to this class's genuinely no-arg {@link #stop()}, which stops the underlying
 * server immediately (zero-second grace period).
 */
public final class MetricsHttpServer {

    private final HttpServer server;

    MetricsHttpServer(HttpServer server) {
        this.server = server;
    }

    /** Stops the underlying HTTP server immediately (zero-second grace period). */
    public void stop() {
        server.stop(0);
    }
}
