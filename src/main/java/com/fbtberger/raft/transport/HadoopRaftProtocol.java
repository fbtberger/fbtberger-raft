/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

public interface HadoopRaftProtocol extends VersionedProtocol {
    long versionID = 1L;

    BytesWritable requestVote(BytesWritable request) throws IOException;
    BytesWritable appendEntries(BytesWritable request) throws IOException;
    BytesWritable installSnapshot(BytesWritable request) throws IOException;
    BytesWritable preVote(BytesWritable request) throws IOException;
    BytesWritable timeoutNow(BytesWritable request) throws IOException;
}
