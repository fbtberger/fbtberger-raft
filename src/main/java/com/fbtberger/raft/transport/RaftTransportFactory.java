package com.fbtberger.raft.transport;

@FunctionalInterface
public interface RaftTransportFactory {
    RaftTransport connect(String address);
}
