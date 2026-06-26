package com.fbtberger.raft.transport;

import io.grpc.ManagedChannelBuilder;

public final class GrpcTransportFactory implements RaftTransportFactory {

    @Override
    public RaftTransport connect(String address) {
        return new GrpcTransport(
                ManagedChannelBuilder.forTarget(address).usePlaintext().build());
    }
}
