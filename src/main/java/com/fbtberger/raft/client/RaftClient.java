/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.client;

import com.fbtberger.raft.client.proto.AddLearnerRequest;
import com.fbtberger.raft.client.proto.AddServerRequest;
import com.fbtberger.raft.client.proto.PromoteLearnerRequest;
import com.fbtberger.raft.client.proto.QueryRequest;
import com.fbtberger.raft.client.proto.QueryResponse;
import com.fbtberger.raft.client.proto.RaftClientServiceGrpc;
import com.fbtberger.raft.client.proto.ReconfigurationResponse;
import com.fbtberger.raft.client.proto.RemoveLearnerRequest;
import com.fbtberger.raft.client.proto.RemoveServerRequest;
import com.fbtberger.raft.client.proto.SubmitRequest;
import com.fbtberger.raft.client.proto.SubmitResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A generic, reusable client for talking to a Raft cluster over the
 * {@code RaftClientService} gRPC contract (client.proto).
 * <p>
 * "Generic" here means this class knows nothing about what a command byte string
 * means -- it just gets it to the current leader and hands back whatever byte
 * string the state machine returned. That keeps it reusable across very different
 * client applications: see {@link KeyValueClient} for one built on top of it, using
 * this repo's demo "SET key value" text protocol. A different deployment with a
 * different state machine can write its own thin wrapper the same way, without
 * reimplementing leader discovery, retries, or transport.
 * <p>
 * Leader discovery works the way §8 of the paper describes: try a node, and if it
 * isn't the leader it tells you (via {@code leader_hint}) who it believes is; this
 * class remembers that hint and tries it next, falling back to round-robining
 * through every configured node if a hint isn't available or turns out to be stale.
 * Not thread-safe across concurrent {@link #submit} calls from multiple threads
 * sharing one instance -- create one {@link RaftClient} per thread, or add your own
 * synchronization, if you need that.
 * <p>
 * {@link #addServer} and {@link #removeServer} let a client drive §6 cluster
 * reconfiguration the same way it submits commands. One limitation worth knowing:
 * {@code clusterAddresses} is fixed at construction time, so if a {@code leader_hint}
 * ever names a node added to the cluster <em>after</em> this client was built, this
 * class has no address for it and falls back to round-robining the nodes it already
 * knows -- it won't discover the new node on its own. As long as at least one of the
 * originally configured nodes is still part of the cluster, that's enough to keep
 * working; just don't expect a brand-new member to get used directly until you also
 * update {@code clusterAddresses}.
 */
public final class RaftClient implements AutoCloseable {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(2);

    /**
     * What a node that is not the leader answers -- set verbatim by
     * {@code RaftClientGrpcService}, and the one rejection that carries no information
     * about the request itself.
     */
    private static final String NOT_LEADER = "not leader";

    private final Map<String, String> clusterAddresses; // nodeId -> host:port
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private volatile String knownLeaderId; // best guess; null means "unknown, try everyone"

    /**
     * @param clusterAddresses every node's id mapped to its "host:port" gRPC
     *                         address -- typically the same address book the
     *                         servers themselves use, e.g. a node's own
     *                         {@code RaftConfig.peerAddresses()}.
     */
    public RaftClient(Map<String, String> clusterAddresses) {
        if (clusterAddresses.isEmpty()) {
            throw new IllegalArgumentException("clusterAddresses must not be empty");
        }
        this.clusterAddresses = new LinkedHashMap<>(clusterAddresses);
    }

    /** Submits a command with the default timeout per attempted node. */
    public byte[] submit(byte[] command) throws RaftClientException {
        return submit(command, DEFAULT_TIMEOUT);
    }

    /**
     * Submits a command, trying the last known (or guessed) leader first, then
     * every other configured node in turn, until one of them accepts it or all
     * of them have been tried.
     *
     * @param perAttemptTimeout how long to wait for each individual node before
     *                          moving on to the next candidate
     * @return the state machine's result for this command, once committed
     * @throws RaftClientException if no node in the cluster accepted the command
     */
    public byte[] submit(byte[] command, Duration perAttemptTimeout) throws RaftClientException {
        SubmitRequest request = SubmitRequest.newBuilder().setCommand(ByteString.copyFrom(command)).build();
        RaftClientException lastError = null;

        for (String nodeId : candidateOrder()) {
            try {
                RaftClientServiceGrpc.RaftClientServiceBlockingStub stub = stubFor(nodeId)
                        .withDeadlineAfter(perAttemptTimeout.toMillis(), TimeUnit.MILLISECONDS);
                SubmitResponse response = stub.submit(request);

                if (response.getSuccess()) {
                    knownLeaderId = nodeId;
                    return response.getResult().toByteArray();
                }
                knownLeaderId = response.getLeaderHint().isEmpty() ? null : response.getLeaderHint();
                lastError = new RaftClientException(
                        nodeId + " rejected the command (" + (response.getError().isEmpty() ? "not leader" : response.getError()) + ")");
            } catch (StatusRuntimeException e) {
                knownLeaderId = null; // an unreachable node is not a useful guess anymore
                lastError = new RaftClientException("could not reach " + nodeId + " (" + clusterAddresses.get(nodeId) + ")", e);
            }
        }
        throw lastError != null ? lastError : new RaftClientException("no nodes configured");
    }

    /** Runs a linearizable read with the default timeout per attempted node. */
    public byte[] query(byte[] query) throws RaftClientException {
        return query(query, DEFAULT_TIMEOUT);
    }

    /**
     * Runs a linearizable read (§6.4), following the same leader-hint walk as
     * {@link #submit}: reads are leader-only here, because a follower can be
     * arbitrarily far behind and a partitioned one need not know it was deposed.
     *
     * @return the state machine's answer
     * @throws RaftClientException if no node in the cluster answered the query
     */
    public byte[] query(byte[] query, Duration perAttemptTimeout) throws RaftClientException {
        QueryRequest request = QueryRequest.newBuilder().setQuery(ByteString.copyFrom(query)).build();
        RaftClientException lastError = null;

        for (String nodeId : candidateOrder()) {
            try {
                QueryResponse response = stubFor(nodeId)
                        .withDeadlineAfter(perAttemptTimeout.toMillis(), TimeUnit.MILLISECONDS)
                        .query(request);

                if (response.getSuccess()) {
                    knownLeaderId = nodeId;
                    return response.getResult().toByteArray();
                }
                knownLeaderId = response.getLeaderHint().isEmpty() ? null : response.getLeaderHint();
                lastError = new RaftClientException(
                        nodeId + " rejected the query (" + (response.getError().isEmpty() ? "not leader" : response.getError()) + ")");
            } catch (StatusRuntimeException e) {
                knownLeaderId = null;
                lastError = new RaftClientException("could not reach " + nodeId + " (" + clusterAddresses.get(nodeId) + ")", e);
            }
        }
        throw lastError != null ? lastError : new RaftClientException("no nodes configured");
    }

    /** Adds a new voting member to the cluster (§6), with the default timeout per attempted node. */
    public void addServer(String id, String address) throws RaftClientException {
        addServer(id, address, DEFAULT_TIMEOUT);
    }

    /** Adds a new voting member to the cluster (§6). The new server should already be running with an empty data directory. */
    public void addServer(String id, String address, Duration perAttemptTimeout) throws RaftClientException {
        AddServerRequest request = AddServerRequest.newBuilder().setId(id).setAddress(address).build();
        reconfigure(nodeId -> stubFor(nodeId)
                .withDeadlineAfter(perAttemptTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .addServer(request));
    }

    /** Removes an existing voting member from the cluster (§6), with the default timeout per attempted node. */
    public void removeServer(String id) throws RaftClientException {
        removeServer(id, DEFAULT_TIMEOUT);
    }

    /** Removes an existing voting member from the cluster (§6). */
    public void removeServer(String id, Duration perAttemptTimeout) throws RaftClientException {
        RemoveServerRequest request = RemoveServerRequest.newBuilder().setId(id).build();
        reconfigure(nodeId -> stubFor(nodeId)
                .withDeadlineAfter(perAttemptTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .removeServer(request));
    }

    /** Adds a non-voting learner (§4.2.1), with the default timeout per attempted node. */
    public void addLearner(String id, String address) throws RaftClientException {
        addLearner(id, address, DEFAULT_TIMEOUT);
    }

    /** Adds a non-voting learner (§4.2.1). The new server should already be running, ideally with node.learner=true. */
    public void addLearner(String id, String address, Duration perAttemptTimeout) throws RaftClientException {
        AddLearnerRequest request = AddLearnerRequest.newBuilder().setId(id).setAddress(address).build();
        reconfigure(nodeId -> stubFor(nodeId)
                .withDeadlineAfter(perAttemptTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .addLearner(request));
    }

    /** Promotes a caught-up learner to a voting member (§4.2.1), with the default timeout per attempted node. */
    public void promoteLearner(String id) throws RaftClientException {
        promoteLearner(id, DEFAULT_TIMEOUT);
    }

    /** Promotes a caught-up learner to a voting member (§4.2.1). Rejected until the learner has caught up. */
    public void promoteLearner(String id, Duration perAttemptTimeout) throws RaftClientException {
        PromoteLearnerRequest request = PromoteLearnerRequest.newBuilder().setId(id).build();
        reconfigure(nodeId -> stubFor(nodeId)
                .withDeadlineAfter(perAttemptTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .promoteLearner(request));
    }

    /** Removes a non-voting learner (§4.2.1), with the default timeout per attempted node. */
    public void removeLearner(String id) throws RaftClientException {
        removeLearner(id, DEFAULT_TIMEOUT);
    }

    /** Removes a non-voting learner (§4.2.1). */
    public void removeLearner(String id, Duration perAttemptTimeout) throws RaftClientException {
        RemoveLearnerRequest request = RemoveLearnerRequest.newBuilder().setId(id).build();
        reconfigure(nodeId -> stubFor(nodeId)
                .withDeadlineAfter(perAttemptTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .removeLearner(request));
    }

    /**
     * Shared retry loop for {@link #addServer} / {@link #removeServer} and the
     * learner operations: the same
     * leader-hint-following strategy as {@link #submit}, just adapted to the
     * reconfiguration RPCs' response shape (no result payload, only success/failure).
     */
    private void reconfigure(Function<String, ReconfigurationResponse> call) throws RaftClientException {
        RaftClientException lastError = null;
        RaftClientException reasoned = null;

        for (String nodeId : candidateOrder()) {
            try {
                ReconfigurationResponse response = call.apply(nodeId);
                if (response.getSuccess()) {
                    knownLeaderId = nodeId;
                    return;
                }
                knownLeaderId = response.getLeaderHint().isEmpty() ? null : response.getLeaderHint();
                RaftClientException rejection = new RaftClientException(
                        nodeId + " rejected the request (" + (response.getError().isEmpty() ? "not leader" : response.getError()) + ")");
                // Keep the first answer that says something beyond "not leader". The walk
                // asks the leader first and every follower afterwards, so overwriting on
                // each rejection means the caller is shown the last follower's "not
                // leader" -- structurally the least informative answer available, and the
                // one that hides the leader's actual objection. Measured twice against a
                // live cluster: a promotion refused with "a previous configuration change
                // has not committed yet; retry once it has" surfaced as "node1 rejected
                // the request (not leader)", which sent two separate investigations after
                // an election that had never happened. The term had not even changed.
                if (reasoned == null && !NOT_LEADER.equals(response.getError())) {
                    reasoned = rejection;
                }
                lastError = rejection;
            } catch (StatusRuntimeException e) {
                knownLeaderId = null;
                lastError = new RaftClientException("could not reach " + nodeId + " (" + clusterAddresses.get(nodeId) + ")", e);
            }
        }
        if (reasoned != null) {
            throw reasoned;
        }
        throw lastError != null ? lastError : new RaftClientException("no nodes configured");
    }

    /** This client's current best guess at who the leader is, or null if unknown. */
    public String knownLeaderId() {
        return knownLeaderId;
    }

    private List<String> candidateOrder() {
        List<String> order = new ArrayList<>(clusterAddresses.size());
        String guess = knownLeaderId;
        if (guess != null && clusterAddresses.containsKey(guess)) {
            order.add(guess);
        }
        for (String nodeId : clusterAddresses.keySet()) {
            if (!nodeId.equals(guess)) {
                order.add(nodeId);
            }
        }
        return order;
    }

    private RaftClientServiceGrpc.RaftClientServiceBlockingStub stubFor(String nodeId) {
        ManagedChannel channel = channels.computeIfAbsent(nodeId, id ->
                ManagedChannelBuilder.forTarget(clusterAddresses.get(id)).usePlaintext().build());
        return RaftClientServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void close() {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdownNow();
        }
    }
}
