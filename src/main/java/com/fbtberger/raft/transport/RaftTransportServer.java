/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import java.io.IOException;

public interface RaftTransportServer extends AutoCloseable {
    void start() throws IOException;
    @Override void close();
}
