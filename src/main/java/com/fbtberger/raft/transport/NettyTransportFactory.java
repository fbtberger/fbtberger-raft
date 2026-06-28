/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;

import javax.net.ssl.SSLException;

public final class NettyTransportFactory implements RaftTransportFactory, AutoCloseable {

    private final NioEventLoopGroup group = new NioEventLoopGroup(2, r -> {
        Thread t = new Thread(r, "raft-netty-client");
        t.setDaemon(true);
        return t;
    });
    private final SslContext sslContext;

    public NettyTransportFactory() {
        this.sslContext = null;
    }

    public NettyTransportFactory(TlsConfig tlsConfig) throws SSLException {
        this.sslContext = tlsConfig.enabled() ? tlsConfig.buildClientSslContext() : null;
    }

    @Override
    public RaftTransport connect(String address) {
        String[] parts = address.split(":");
        return new NettyTransport(parts[0], Integer.parseInt(parts[1]), group, sslContext);
    }

    @Override
    public void close() {
        group.shutdownGracefully();
    }
}
