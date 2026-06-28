/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;

public final class HadoopTransportFactory implements RaftTransportFactory, AutoCloseable {

    private final Configuration conf;

    public HadoopTransportFactory() {
        this(new Configuration());
    }

    public HadoopTransportFactory(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public RaftTransport connect(String address) {
        String[] parts = address.split(":");
        InetSocketAddress addr = NetUtils.createSocketAddr(parts[0], Integer.parseInt(parts[1]));
        try {
            HadoopRaftProtocol proxy = RPC.getProxy(
                    HadoopRaftProtocol.class,
                    HadoopRaftProtocol.versionID,
                    addr,
                    conf);
            return new HadoopTransport(proxy);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to connect to " + address, e);
        }
    }

    @Override
    public void close() {
    }
}
