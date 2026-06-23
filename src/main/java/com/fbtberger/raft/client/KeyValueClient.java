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

    // Note: there is deliberately no get() here. Reads in this demo are served
    // locally by whichever node you happen to ask (see RaftServer's CLI), which is
    // not linearizable -- a client could read stale data from a partitioned-away
    // follower. Routing reads safely through Raft (e.g. the paper's read-index
    // approach, §8) is out of scope for this implementation; see the README.
}
