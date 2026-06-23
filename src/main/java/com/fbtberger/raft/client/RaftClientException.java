package com.fbtberger.raft.client;

/**
 * Thrown by {@link RaftClient} when a command could not be submitted -- either
 * because no reachable node in the cluster currently accepted it as leader, or
 * because the leader that did accept it reported an error applying it. Checked
 * deliberately: a caller talking to a distributed system should decide explicitly
 * whether and how to retry, rather than have that decision made implicitly by
 * letting an unchecked exception propagate.
 */
public final class RaftClientException extends Exception {

    public RaftClientException(String message) {
        super(message);
    }

    public RaftClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
