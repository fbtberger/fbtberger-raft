/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.ClientSession;
import com.fbtberger.raft.proto.ClientSessionResult;
import com.fbtberger.raft.proto.ClientSessionTable;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * The replicated table of client sessions that makes a submitted command take effect
 * exactly once (paper §8, dissertation §6.3).
 *
 * <p>Raft alone gives at-least-once: a leader can commit a command and crash before it
 * answers, the client retries against the next leader, and the command is in the log
 * twice. Each client therefore tags a command with its own id and a sequence number, and
 * this table remembers, per client, the result of every sequence the client has not yet
 * acknowledged. A tag seen before is answered from here and never reaches the state
 * machine again.
 *
 * <h2>Why this is replicated state and not a leader-side cache</h2>
 * The table is changed only by {@link #begin} and {@link #record}, which
 * {@code RaftNode} calls while applying committed entries, in log order. So every server
 * holds the same table at the same index, and it travels in the snapshot beside the
 * state machine's own data. A cache on the leader would be empty on exactly the server
 * the retry is sent to.
 *
 * <h2>Bounded, deterministically</h2>
 * A result is dropped once the client acknowledges it. A client that never comes back is
 * dropped when {@value #MAX_SESSIONS} others have been active since, least recently used
 * first. That bound is a constant on purpose: eviction decides whether a command is
 * applied, so a per-node setting would let two servers decide differently. The price is
 * stated rather than hidden -- a client whose retry is still in flight while
 * {@value #MAX_SESSIONS} other clients commit loses its protection for that one retry.
 *
 * <p>Not thread-safe; {@code RaftNode} touches it only under its lock.
 */
final class ClientSessions {

    /** How many clients are remembered at once. A constant, see the class comment. */
    static final int MAX_SESSIONS = 4096;

    /** What {@link #begin} found out about a tagged command. */
    enum Kind {
        /** Not seen before: apply it, then {@link #record} the result. */
        NEW,
        /** Applied before and not yet acknowledged: answer with {@link Lookup#result}. */
        DUPLICATE,
        /** At or below what the client acknowledged: its result is gone, do not apply. */
        STALE
    }

    record Lookup(Kind kind, byte[] result) {}

    private static final Lookup NEW = new Lookup(Kind.NEW, null);
    private static final Lookup STALE = new Lookup(Kind.STALE, null);

    private static final class Session {
        long ackedSequence;
        final TreeMap<Long, byte[]> results = new TreeMap<>();
    }

    private final int maxSessions;
    // Access-ordered: iteration runs from least to most recently used.
    private final LinkedHashMap<String, Session> sessions = new LinkedHashMap<>(16, 0.75f, true);

    ClientSessions() {
        this(MAX_SESSIONS);
    }

    /** For tests, which cannot reasonably create {@value #MAX_SESSIONS} clients. */
    ClientSessions(int maxSessions) {
        this.maxSessions = maxSessions;
    }

    /**
     * Looks a committed command up and takes note of the client's acknowledgement. Must
     * be called for every tagged entry, in log order, on every server.
     */
    Lookup begin(String clientId, long sequence, long ackedSequence) {
        Session session = sessions.get(clientId);
        if (session == null) {
            session = new Session();
            sessions.put(clientId, session);
            evictBeyondLimit();
        }
        if (ackedSequence > session.ackedSequence) {
            session.ackedSequence = ackedSequence;
            session.results.headMap(ackedSequence, true).clear();
        }
        if (sequence <= session.ackedSequence) {
            return STALE;
        }
        byte[] recorded = session.results.get(sequence);
        return recorded != null ? new Lookup(Kind.DUPLICATE, recorded) : NEW;
    }

    /** Remembers the result of a command {@link #begin} reported as {@link Kind#NEW}. */
    void record(String clientId, long sequence, byte[] result) {
        Session session = sessions.get(clientId);
        if (session != null) {
            session.results.put(sequence, result == null ? new byte[0] : result);
        }
    }

    int size() {
        return sessions.size();
    }

    private void evictBeyondLimit() {
        Iterator<Map.Entry<String, Session>> it = sessions.entrySet().iterator();
        while (sessions.size() > maxSessions && it.hasNext()) {
            it.next();
            it.remove();
        }
    }

    /** The whole table, least recently used first, for a snapshot. */
    byte[] toBytes() {
        ClientSessionTable.Builder table = ClientSessionTable.newBuilder();
        for (Map.Entry<String, Session> e : sessions.entrySet()) {
            ClientSession.Builder session = ClientSession.newBuilder()
                    .setClientId(e.getKey())
                    .setAckedSequence(e.getValue().ackedSequence);
            for (Map.Entry<Long, byte[]> r : e.getValue().results.entrySet()) {
                session.addResults(ClientSessionResult.newBuilder()
                        .setSequence(r.getKey())
                        .setResult(ByteString.copyFrom(r.getValue())));
            }
            table.addSessions(session);
        }
        return table.build().toByteArray();
    }

    /**
     * Replaces the table with what a snapshot holds. Empty input is an empty table, which
     * is what a snapshot written before sessions existed carries.
     */
    void restore(byte[] data) {
        sessions.clear();
        if (data == null || data.length == 0) {
            return;
        }
        ClientSessionTable table;
        try {
            table = ClientSessionTable.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("corrupt client session table in snapshot", e);
        }
        for (ClientSession stored : table.getSessionsList()) {
            Session session = new Session();
            session.ackedSequence = stored.getAckedSequence();
            for (ClientSessionResult r : stored.getResultsList()) {
                session.results.put(r.getSequence(), r.getResult().toByteArray());
            }
            sessions.put(stored.getClientId(), session);
        }
    }
}
