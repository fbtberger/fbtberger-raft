package com.fbtberger.raft.transport;

import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;

import javax.net.ssl.SSLException;

public final class GrpcTransportFactory implements RaftTransportFactory {

    private final TlsConfig tlsConfig;

    public GrpcTransportFactory() {
        this(TlsConfig.disabled());
    }

    public GrpcTransportFactory(TlsConfig tlsConfig) {
        this.tlsConfig = tlsConfig;
    }

    @Override
    public RaftTransport connect(String address) {
        if (!tlsConfig.enabled()) {
            return new GrpcTransport(
                    ManagedChannelBuilder.forTarget(address).usePlaintext().build());
        }
        try {
            var sslContext = GrpcSslContexts.forClient()
                    .keyManager(tlsConfig.certFile(), tlsConfig.keyFile())
                    .trustManager(tlsConfig.caFile())
                    .build();
            return new GrpcTransport(
                    NettyChannelBuilder.forTarget(address).sslContext(sslContext).build());
        } catch (SSLException e) {
            throw new RuntimeException("failed to create TLS channel to " + address, e);
        }
    }
}
