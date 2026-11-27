/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.client;

import com.fbtberger.raft.client.proto.AddLearnerRequest;
import com.fbtberger.raft.client.proto.AddServerRequest;
import com.fbtberger.raft.client.proto.PromoteLearnerRequest;
import com.fbtberger.raft.client.proto.TransferLeadershipRequest;
import com.fbtberger.raft.client.proto.QueryRequest;
import com.fbtberger.raft.client.proto.QueryResponse;
import com.fbtberger.raft.client.proto.RaftClientServiceGrpc;
import com.fbtberger.raft.client.proto.ReconfigurationResponse;
import com.fbtberger.raft.client.proto.RemoveLearnerRequest;
import com.fbtberger.raft.client.proto.RemoveServerRequest;
import com.fbtberger.raft.client.proto.SubmitRequest;
import com.fbtberger.raft.client.proto.SubmitResponse;
import com.google.protobuf.ByteString;
import com.fbtberger.raft.auth.BearerTokenInterceptor;
import com.fbtberger.raft.transport.TlsConfig;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import javax.net.ssl.SSLException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

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
    /** How often an overloaded leader is asked again before the caller is told. */
    static final int MAX_OVERLOAD_RETRIES = 5;
    /** The longest single pause between two of those attempts. */
    static final long MAX_BACKOFF_MS = 1_000;

    /**
     * Waits before asking an overloaded leader again: the pause it suggested, doubled per
     * refusal, capped, and spread by up to half so that the clients it refused together do
     * not all come back together.
     */
    private static void backOff(long suggestedMs, int refusals) throws RaftClientException {
        long base = Math.min(MAX_BACKOFF_MS, Math.max(1, suggestedMs) << Math.min(refusals, 10));
        long pause = base / 2 + java.util.concurrent.ThreadLocalRandom.current().nextLong(base / 2 + 1);
        try {
            Thread.sleep(pause);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RaftClientException("interrupted while waiting for an overloaded leader", e);
        }
    }

    /**
     * What a node that is not the leader answers -- set verbatim by
     * {@code RaftClientGrpcService}, and the one rejection that carries no information
     * about the request itself.
     */
    private static final String NOT_LEADER = "not leader";

    private final Map<String, String> clusterAddresses; // nodeId -> host:port
    private final TlsConfig tlsConfig;
    /** Supplies the caller's current Keycloak access token, or null if the cluster asks for none. */
    private final Supplier<String> bearerToken;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private volatile String knownLeaderId; // best guess; null means "unknown, try everyone"

    /**
     * This client's session (§8). Every command gets the next sequence number and keeps it
     * across every retry, so the cluster can tell a retry from a new command and apply it
     * once. A fresh random id per instance: two instances are two clients, and a process
     * that restarts starts a new session rather than guessing where the old one stopped.
     */
    private final String clientId = UUID.randomUUID().toString();
    private final AtomicLong nextSequence = new AtomicLong(1);
    private final AtomicLong followerReadRotation = new AtomicLong();
    /**
     * Sequences submitted and not yet answered or given up. Everything below the lowest is
     * acknowledged -- the lowest, not the newest, so that sharing an instance between threads
     * cannot acknowledge a command another thread is still retrying.
     */
    private final ConcurrentSkipListSet<Long> outstanding = new ConcurrentSkipListSet<>();

    /**
     * A plaintext client, which is what this class used to be able to be and nothing else.
     *
     * @param clusterAddresses every node's id mapped to its "host:port" gRPC
     *                         address -- typically the same address book the
     *                         servers themselves use, e.g. a node's own
     *                         {@code RaftConfig.peerAddresses()}.
     */
    public RaftClient(Map<String, String> clusterAddresses) {
        this(clusterAddresses, TlsConfig.disabled());
    }

    /**
     * A client that speaks the same transport security as the cluster's peers.
     *
     * <p>The peer transport has had TLS since it was written, and on the kwatro production
     * cluster it is switched on -- all five members run mTLS. This channel did not have the
     * option at all: {@code usePlaintext()} was hard-coded, so the commands the web tier
     * forwards with {@code SubmitRaftCommand}, and every membership change an operator makes,
     * crossed the same network in the clear while the heartbeats beside them were encrypted
     * and mutually authenticated. That asymmetry was not a decision anyone took; it was a
     * constructor that could not express the other case.
     *
     * <p>Deliberately the same {@link TlsConfig} the servers read from their own properties,
     * and the same builder idiom as {@code GrpcTransportFactory}, so a cluster and the tools
     * that talk to it cannot end up configured by two different vocabularies.
     *
     * <p>Note what this does <em>not</em> do: a client certificate proves who is calling, and
     * nothing here checks that the caller is allowed to reconfigure the cluster. mTLS on this
     * channel narrows the set of people who can reach it to those holding a cluster
     * certificate; it is not authorisation.
     */
    public RaftClient(Map<String, String> clusterAddresses, TlsConfig tlsConfig) {
        this(clusterAddresses, tlsConfig, null);
    }

    /**
     * A client that also says who it is: every RPC carries the token {@code bearerToken}
     * returns at that moment, which is what a cluster with {@code auth.keycloak.enabled=true}
     * checks before it lets the RPC through. This is the authorisation the constructor
     * above says mTLS is not.
     *
     * <p>The supplier is asked per call, so it can hand out a refreshed token without the
     * client being rebuilt -- {@code KeycloakTokenSupplier} for a service account, or the
     * token of the user on whose behalf the caller acts.
     *
     * @param bearerToken supplies the access token without the {@code Bearer } prefix;
     *                    null for a cluster that requires none
     */
    public RaftClient(Map<String, String> clusterAddresses, TlsConfig tlsConfig, Supplier<String> bearerToken) {
        if (clusterAddresses.isEmpty()) {
            throw new IllegalArgumentException("clusterAddresses must not be empty");
        }
        this.clusterAddresses = new LinkedHashMap<>(clusterAddresses);
        this.tlsConfig = tlsConfig;
        this.bearerToken = bearerToken;
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
        // One sequence number for the whole walk below. That is the point of it: the
        // attempt that times out against a leader which committed the command and the
        // attempt that then reaches its successor are the same command to the cluster.
        long sequence;
        long acked;
        // Numbered, registered and acknowledged-up-to in one step. Apart, a second
        // thread could compute its acknowledgement between this thread taking a number
        // and registering it, and acknowledge a command that has not been sent yet.
        synchronized (outstanding) {
            sequence = nextSequence.getAndIncrement();
            outstanding.add(sequence);
            acked = outstanding.first() - 1;
        }
        try {
            return submitTagged(command, sequence, acked, perAttemptTimeout);
        } finally {
            // Answered or given up: either way this client never asks for it again, so
            // the servers may drop its result once a later command carries the news.
            outstanding.remove(sequence);
        }
    }

    private byte[] submitTagged(byte[] command, long sequence, long acked, Duration perAttemptTimeout) throws RaftClientException {
        SubmitRequest request = SubmitRequest.newBuilder()
                .setCommand(ByteString.copyFrom(command))
                .setClientId(clientId)
                .setSequence(sequence)
                .setAckedSequence(acked)
                .build();
        RaftClientException lastError = null;

        for (String nodeId : candidateOrder()) {
            try {
                SubmitResponse response = null;
                // The same node again while it says "overloaded": it is the leader, and
                // the nodes after it in the walk are followers that can only say no. The
                // command was not appended, and it carries the same sequence number each
                // time, so asking again cannot apply it twice.
                for (int refusals = 0; ; refusals++) {
                    response = stubFor(nodeId)
                            .withDeadlineAfter(perAttemptTimeout.toMillis(), TimeUnit.MILLISECONDS)
                            .submit(request);
                    if (!response.getOverloaded()) {
                        break;
                    }
                    knownLeaderId = nodeId;
                    if (refusals == MAX_OVERLOAD_RETRIES) {
                        throw RaftClientException.overloaded(nodeId + " is overloaded and refused the command "
                                + (refusals + 1) + " times (" + response.getError() + ")");
                    }
                    backOff(response.getRetryAfterMs(), refusals);
                }

                if (response.getSuccess()) {
                    knownLeaderId = nodeId;
                    return response.getResult().toByteArray();
                }
                knownLeaderId = response.getLeaderHint().isEmpty() ? null : response.getLeaderHint();
                lastError = new RaftClientException(
                        nodeId + " rejected the command (" + (response.getError().isEmpty() ? "not leader" : response.getError()) + ")");
            } catch (StatusRuntimeException e) {
                throwIfNotAuthorised(nodeId, e);
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

    /** Runs a linearizable read through {@link ReadMode#READ_INDEX}. */
    public byte[] query(byte[] query, Duration perAttemptTimeout) throws RaftClientException {
        return query(query, perAttemptTimeout, ReadMode.READ_INDEX);
    }

    /**
     * Runs a linearizable read (§6.4), following the same leader-hint walk as
     * {@link #submit}: reads are leader-only here, because a follower can be
     * arbitrarily far behind and a partitioned one need not know it was deposed.
     *
     * @return the state machine's answer
     * @throws RaftClientException if no node in the cluster answered the query
     */
    public byte[] query(byte[] query, Duration perAttemptTimeout, ReadMode mode)
            throws RaftClientException {
        // An old server ignores the lease field and answers a ReadIndex read successfully,
        // and the response carries no record of which path ran. A lease measurement against
        // a cluster that predates the field therefore measures ReadIndex and says so
        // nowhere -- check the server version, not the exit status.
        QueryRequest request = QueryRequest.newBuilder()
                .setQuery(ByteString.copyFrom(query))
                .setLease(mode == ReadMode.LEASE)
                .setAllowFollower(mode == ReadMode.FOLLOWER)
                .build();
        RaftClientException lastError = null;
        boolean followerRead = mode == ReadMode.FOLLOWER;

        for (String nodeId : followerRead ? followerFirstOrder() : candidateOrder()) {
            try {
                QueryResponse response = null;
                for (int refusals = 0; ; refusals++) {
                    response = stubFor(nodeId)
                            .withDeadlineAfter(perAttemptTimeout.toMillis(), TimeUnit.MILLISECONDS)
                            .query(request);
                    if (!response.getOverloaded()) {
                        break;
                    }
                    if (refusals == MAX_OVERLOAD_RETRIES) {
                        throw RaftClientException.overloaded(nodeId + " is overloaded and refused the query "
                                + (refusals + 1) + " times (" + response.getError() + ")");
                    }
                    backOff(response.getRetryAfterMs(), refusals);
                }

                if (response.getSuccess()) {
                    // Whoever answered a follower read need not be the leader, and must
                    // not be remembered as one: the next submit would start its walk there.
                    if (!followerRead) {
                        knownLeaderId = nodeId;
                    }
                    return response.getResult().toByteArray();
                }
                if (!response.getLeaderHint().isEmpty()) {
                    knownLeaderId = response.getLeaderHint();
                } else if (!followerRead) {
                    knownLeaderId = null;
                }
                lastError = new RaftClientException(
                        nodeId + " rejected the query (" + (response.getError().isEmpty() ? "not leader" : response.getError()) + ")");
            } catch (StatusRuntimeException e) {
                throwIfNotAuthorised(nodeId, e);
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

    /** Hands leadership to another member (§3.10), with the default timeout per attempted node. */
    public void transferLeadership(String targetId) throws RaftClientException {
        transferLeadership(targetId, DEFAULT_TIMEOUT);
    }

    /**
     * Asks the leader to hand leadership to {@code targetId} (§3.10): it stops taking new
     * commands, brings the target's log level with its own, and tells it to stand for
     * election immediately -- which the target's voters honour despite leader stickiness,
     * because the incumbent asked for it.
     *
     * <p>The target must be a voting member. Returning normally means the target was told
     * to campaign, not that it has won; the election is a cluster-wide event with its own
     * timing, and no request/response can honestly claim it. Confirm by reading the roles.
     *
     * <p>The alternative -- stopping the leader and letting the timeout elect someone --
     * puts the choice of successor up to chance and costs an unavailability window. This
     * costs neither.
     */
    public void transferLeadership(String targetId, Duration perAttemptTimeout) throws RaftClientException {
        TransferLeadershipRequest request = TransferLeadershipRequest.newBuilder().setTargetId(targetId).build();
        reconfigure(nodeId -> stubFor(nodeId)
                .withDeadlineAfter(perAttemptTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .transferLeadership(request));
    }

    /**
     * Shared retry loop for {@link #addServer} / {@link #removeServer}, the learner
     * operations and {@link #transferLeadership}: the same
     * leader-hint-following strategy as {@link #submit}, just adapted to the
     * reconfiguration RPCs' response shape (no result payload, only success/failure).
     *
     * <p>A leadership transfer is not a reconfiguration, and shares this deliberately: it
     * is leader-only, it is refused with a leader hint by everyone else, and its rejections
     * carry a reason. Those are the only properties this loop uses.
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
                throwIfNotAuthorised(nodeId, e);
                knownLeaderId = null;
                lastError = new RaftClientException("could not reach " + nodeId + " (" + clusterAddresses.get(nodeId) + ")", e);
            }
        }
        if (reasoned != null) {
            throw reasoned;
        }
        throw lastError != null ? lastError : new RaftClientException("no nodes configured");
    }

    /** The id this client tags its commands with (§8). */
    public String clientId() {
        return clientId;
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

    /**
     * For a follower read: every node that is not the known leader, starting one further
     * along on each call so the reads spread, and the leader last as the fallback that
     * can always answer.
     */
    private List<String> followerFirstOrder() {
        String leader = knownLeaderId;
        List<String> others = new ArrayList<>(clusterAddresses.size());
        for (String nodeId : clusterAddresses.keySet()) {
            if (!nodeId.equals(leader)) {
                others.add(nodeId);
            }
        }
        if (!others.isEmpty()) {
            java.util.Collections.rotate(others, -(int) (followerReadRotation.getAndIncrement() % others.size()));
        }
        if (leader != null && clusterAddresses.containsKey(leader)) {
            others.add(leader);
        }
        return others;
    }

    private RaftClientServiceGrpc.RaftClientServiceBlockingStub stubFor(String nodeId) {
        ManagedChannel channel = channels.computeIfAbsent(nodeId, this::openChannel);
        RaftClientServiceGrpc.RaftClientServiceBlockingStub stub = RaftClientServiceGrpc.newBlockingStub(channel);
        return bearerToken == null ? stub : stub.withInterceptors(new BearerTokenInterceptor(bearerToken));
    }

    /**
     * A refusal for lack of a token or a role ends the walk at once. Every node applies
     * the same rule to the same token, so asking the others cannot succeed -- and letting
     * the loop run on would report "could not reach node3", which sends the reader after
     * a network fault that is not there.
     */
    private static void throwIfNotAuthorised(String nodeId, StatusRuntimeException e) throws RaftClientException {
        Status.Code code = e.getStatus().getCode();
        if (code == Status.Code.UNAUTHENTICATED || code == Status.Code.PERMISSION_DENIED) {
            String detail = e.getStatus().getDescription();
            throw new RaftClientException(nodeId + " refused the request: "
                    + (code == Status.Code.UNAUTHENTICATED ? "not authenticated" : "not authorised")
                    + (detail == null ? "" : " (" + detail + ")"), e);
        }
    }

    /** Same shape as {@code GrpcTransportFactory.connect}, so both ends read alike. */
    private ManagedChannel openChannel(String nodeId) {
        String address = clusterAddresses.get(nodeId);
        if (!tlsConfig.enabled()) {
            return ManagedChannelBuilder.forTarget(address).usePlaintext().build();
        }
        try {
            var ssl = GrpcSslContexts.forClient();
            // A client certificate only when the cluster asks for one. Against a one-way-TLS
            // server, offering one is harmless; requiring the operator to have one when the
            // server never checks would be a configuration people work around.
            if (tlsConfig.mtlsEnabled()) {
                ssl.keyManager(tlsConfig.material().keyManager());
            }
            return NettyChannelBuilder.forTarget(address)
                    .sslContext(ssl.trustManager(tlsConfig.caFile()).build())
                    .build();
        } catch (SSLException e) {
            throw new IllegalStateException("failed to create TLS channel to " + nodeId
                    + " (" + address + ")", e);
        }
    }

    @Override
    public void close() {
        for (ManagedChannel channel : channels.values()) {
            channel.shutdownNow();
        }
    }
}
