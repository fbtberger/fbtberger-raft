/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

@FunctionalInterface
public interface RaftTransportFactory {
    RaftTransport connect(String address);
}
