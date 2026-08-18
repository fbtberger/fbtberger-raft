/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.client;

import java.nio.charset.StandardCharsets;

/**
 * A small example of building a domain-specific client on top of the generic
 * {@link RaftClient}: it knows the demo {@code KeyValueStateMachine}'s
 * "{@code SET key value}" text command format and encodes/decodes accordingly,
 * while leaving all leader discovery, retrying, and transport to RaftClient.
 * A different deployment with a different state machine -- a different command
 * encoding entirely -- would write an equivalent wrapper of its own; nothing about
 * RaftClient itself needs to change.
 */
public final class KeyValueClient {

    private final RaftClient raftClient;

    public KeyValueClient(RaftClient raftClient) {
        this.raftClient = raftClient;
    }

    /**
     * Sets {@code key} to {@code value}, blocking until the command has been
     * committed and applied by the cluster.
     *
     * @return the state machine's reply, e.g. {@code "OK"}
     */
    public String set(String key, String value) throws RaftClientException {
        byte[] command = ("SET " + key + " " + value).getBytes(StandardCharsets.UTF_8);
        byte[] result = raftClient.submit(command);
        return new String(result, StandardCharsets.UTF_8);
    }

    /**
     * Reads {@code key} linearizably: the answer reflects every SET that had
     * committed when the call was made.
     *
     * <p>This replaces the note that used to stand here saying reads were out of
     * scope. They no longer are: {@code RaftClientService.Query} routes the read
     * through the leader's ReadIndex barrier (§6.4) instead of asking whichever
     * node happens to answer, which is what {@code RaftServer}'s local CLI GET
     * still does -- that one can serve stale data from a partitioned-away
     * follower, and is fine only because it is a debugging aid.
     *
     * @return the stored value, or {@code "ERR no such key"}
     */
    public String get(String key) throws RaftClientException {
        byte[] result = raftClient.query(("GET " + key).getBytes(StandardCharsets.UTF_8));
        return new String(result, StandardCharsets.UTF_8);
    }
}
