package com.fbtberger.raft.transport;

import java.io.IOException;

public interface RaftTransportServer extends AutoCloseable {
    void start() throws IOException;
    @Override void close();
}
