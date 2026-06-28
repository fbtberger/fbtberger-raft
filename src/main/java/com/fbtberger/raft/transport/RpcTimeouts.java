/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import java.util.Properties;

public final class RpcTimeouts {

    public static final long DEFAULT_REQUEST_VOTE_MS     = 1_000;
    public static final long DEFAULT_APPEND_ENTRIES_MS   = 2_000;
    public static final long DEFAULT_INSTALL_SNAPSHOT_MS = 30_000;
    public static final long DEFAULT_PRE_VOTE_MS         = 1_000;

    private final long requestVoteMs;
    private final long appendEntriesMs;
    private final long installSnapshotMs;
    private final long preVoteMs;

    public RpcTimeouts(long requestVoteMs, long appendEntriesMs,
                       long installSnapshotMs, long preVoteMs) {
        this.requestVoteMs = requestVoteMs;
        this.appendEntriesMs = appendEntriesMs;
        this.installSnapshotMs = installSnapshotMs;
        this.preVoteMs = preVoteMs;
    }

    public static RpcTimeouts defaults() {
        return new RpcTimeouts(DEFAULT_REQUEST_VOTE_MS, DEFAULT_APPEND_ENTRIES_MS,
                DEFAULT_INSTALL_SNAPSHOT_MS, DEFAULT_PRE_VOTE_MS);
    }

    public static RpcTimeouts fromProperties(Properties props) {
        return new RpcTimeouts(
                parseLong(props, "rpc.timeout.request.vote.ms", DEFAULT_REQUEST_VOTE_MS),
                parseLong(props, "rpc.timeout.append.entries.ms", DEFAULT_APPEND_ENTRIES_MS),
                parseLong(props, "rpc.timeout.install.snapshot.ms", DEFAULT_INSTALL_SNAPSHOT_MS),
                parseLong(props, "rpc.timeout.pre.vote.ms", DEFAULT_PRE_VOTE_MS));
    }

    private static long parseLong(Properties props, String key, long defaultValue) {
        String v = props.getProperty(key);
        return v == null ? defaultValue : Long.parseLong(v);
    }

    public long requestVoteMs()     { return requestVoteMs; }
    public long appendEntriesMs()   { return appendEntriesMs; }
    public long installSnapshotMs() { return installSnapshotMs; }
    public long preVoteMs()         { return preVoteMs; }
}
