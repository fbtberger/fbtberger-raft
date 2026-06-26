package com.fbtberger.raft.transport;

import io.netty.channel.nio.NioEventLoopGroup;

public final class NettyTransportFactory implements RaftTransportFactory, AutoCloseable {

    private final NioEventLoopGroup group = new NioEventLoopGroup(2, r -> {
        Thread t = new Thread(r, "raft-netty-client");
        t.setDaemon(true);
        return t;
    });

    @Override
    public RaftTransport connect(String address) {
        String[] parts = address.split(":");
        return new NettyTransport(parts[0], Integer.parseInt(parts[1]), group);
    }

    @Override
    public void close() {
        group.shutdownGracefully();
    }
}
