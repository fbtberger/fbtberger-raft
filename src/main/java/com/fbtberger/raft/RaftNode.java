/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.ClusterConfiguration;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.LogEntry;
import com.fbtberger.raft.proto.NamedCommand;
import com.fbtberger.raft.proto.PreVoteRequest;
import com.fbtberger.raft.proto.PreVoteResponse;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import com.fbtberger.raft.proto.ReadIndexRequest;
import com.fbtberger.raft.proto.ReadIndexResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import com.fbtberger.raft.transport.RaftTransport;
import com.fbtberger.raft.transport.RaftTransportFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


/**
 * The core Raft algorithm: leader election, log replication, cluster
 * membership changes, and the safety rules that tie them together,
 * following Ongaro and Ousterhout's "In Search of an Understandable
 * Consensus Algorithm" (2014) -- mainly Section 5 (elections and
 * replication) and Section 6 (membership changes).
 *
 * This class owns all in-memory decision making. It asks
 * {@link RaftStorage} to durably store anything that must survive a crash,
 * talks to peers through gRPC stubs it creates on demand via a supplied
 * factory (so it can connect to newly added members on its own), and hands
 * committed commands to a {@link StateMachine}.
 */
public final class RaftNode implements com.fbtberger.raft.transport.RaftRpcHandler {

    private static final Logger LOG = LoggerFactory.getLogger(RaftNode.class);

    private static final int HEARTBEAT_INTERVAL_MS = 50;
    private static final int ELECTION_TIMEOUT_MIN_MS = 150;
    private static final int ELECTION_TIMEOUT_MAX_MS = 300;
    /** How much longer than a data node a witness waits before it stands for election. */
    static final int WITNESS_ELECTION_FACTOR = 3;
    /**
     * How many ordinary election timeouts a node waits before its <em>first</em> campaign after
     * {@link #start()}. Only the first timer is scaled; once anything resets the timer -- which
     * any heartbeat does -- the node is on the normal 150-300 ms schedule.
     *
     * <p>Six, because a restarting node campaigned before the incumbent's transport had
     * reconnected to it and took the leadership away from a leader that was perfectly healthy
     * (issue #2, the requester side of it). Measured: the gap between "starting as FOLLOWER" and
     * the first PreVote was 498-802 ms, so an ordinary timeout expired long before the first
     * heartbeat arrived. Scaling only this one timer buys that window back without slowing down
     * a real failover.
     *
     * <p>This was a configurable switch until 2026-11-16, so that a talk could set it to 1 and
     * show the defect happening. The talk is over; what is left is the fix.
     */
    private static final int BOOT_DELAY_FACTOR = 6;
    // §10.2.2: cap each AppendEntries batch so heartbeats aren't starved.
    private static final int MAX_BATCH_BYTES = 1_048_576; // 1 MB
    // §10.2.2: maximum pipelined AppendEntries RPCs in flight per peer.
    private static final int MAX_INFLIGHT_APPENDS = 2;

    private final RaftConfig config;
    private final RaftStorage store;
    /**
     * The state machines this node drives. On a witness this is a registry that holds
     * nothing but a {@link WitnessStateMachine}, whatever the node was given.
     */
    private final StateMachineRegistry stateMachines;
    /**
     * A witness: a voting member with a log and no state machine. See {@link #isWitness()}.
     * Fixed for the life of the node; it comes from {@code node.witness}.
     */
    private final boolean witness;
    /**
     * Peers that answered a TimeoutNow with "I am a witness". Learned, not configured: the
     * cluster configuration does not say who is one. Only used to choose whom to hand the
     * lead to, so a wrong or missing entry costs one refused transfer and nothing else.
     */
    private final java.util.Set<String> knownWitnesses = ConcurrentHashMap.newKeySet();
    private final RaftTransportFactory transportFactory;
    private final RaftMetrics metrics;
    private final Map<String, RaftTransport> peerTransports = new ConcurrentHashMap<>(); // excludes self
    private final ScheduledExecutorService scheduler;
    /** Snapshot work only; see the constructor for why it is not the scheduler. */
    private final ExecutorService snapshotExecutor;

    private final ReentrantLock lock = new ReentrantLock();
    private volatile ServerRole role = ServerRole.FOLLOWER;
    private volatile String currentLeaderId = null;

    // Volatile state on all servers (Figure 2).
    private final AtomicLong commitIndex = new AtomicLong(0);
    private final AtomicLong lastApplied = new AtomicLong(0);

    // Volatile state on leaders, reinitialized after every election (Figure 2).
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();
    // §10.2.2: in-flight AppendEntries RPCs per peer for pipelining.
    private final Map<String, Integer> peerInflight = new ConcurrentHashMap<>();
    // v101 — replication diagnostics. Until now BOTH ways a peer can fall out of
    // replication were silent: a missing transport made replicateTo() return without a
    // word, and a failed AppendEntries future was swallowed in whenComplete(). A follower
    // could therefore sit at a stale index for hours with nothing in any log to show for
    // it (seen on kwatro-1: log stuck at 336, state machine empty, leader silent).
    private final Map<String, Long> peerLastFailureLogMs = new ConcurrentHashMap<>();
    private final Map<String, String> peerLastFailure = new ConcurrentHashMap<>();
    // v117 — consecutive replication transport failures per peer. A plain gRPC channel that has
    // gone into a persistent io-failure against a peer which was recreated (new container / new
    // IP) does not reliably re-resolve and reconnect on its own — it can hammer the dead
    // connection indefinitely while the peer sits at a stale index (the "learner stuck at 0,
    // leader silent" signature). After MAX_CONSECUTIVE_FAILURES_BEFORE_REBUILD in a row we discard
    // the cached transport and build a fresh one; reset to 0 on any success or after a rebuild.
    private final Map<String, Integer> peerConsecutiveFailures = new ConcurrentHashMap<>();
    private static final int MAX_CONSECUTIVE_FAILURES_BEFORE_REBUILD = 3;
    // Bound the churn: the common case is ONE dead peer under a healthy leader, which would
    // otherwise rebuild every few heartbeats until the peer returns. Rebuild at most this often
    // per peer; the first rebuild after an outage still fires immediately (lastRebuild defaults 0).
    private final Map<String, Long> peerLastRebuildMs = new ConcurrentHashMap<>();
    private static final long REBUILD_COOLDOWN_MS = 2_000;
    private volatile long lastReplicationStatusLogMs = 0;
    /** Highest commitIndex the leader has told us about — lets a follower know it is behind (v105). */
    private volatile long leaderCommitSeen = 0;
    private static final long FAILURE_LOG_THROTTLE_MS = 5_000;
    private static final long REPLICATION_STATUS_INTERVAL_MS = 10_000;

    // §10.2.1: tracks how far the leader's own disk writes have been
    // durably synced, so the leader can replicate to followers in
    // parallel with writing to its own disk.
    private final AtomicLong leaderDiskMatchIndex = new AtomicLong(0);
    // §4 errata: index of the no-op appended when this leader took office.
    // Config changes are rejected until commitIndex >= this value.
    private volatile long leaderNoOpIndex = Long.MAX_VALUE;

    private final Map<Long, CompletableFuture<byte[]>> pendingClientRequests = new ConcurrentHashMap<>();

    /**
     * Client sessions (§8): which tagged commands have been applied, and with what result.
     * Replicated state -- changed only in {@link #applyCommittedEntries}, saved in every
     * snapshot, replaced whenever one is restored. Guarded by {@code lock}.
     */
    private final ClientSessions clientSessions = new ClientSessions();

    // Leader-side: in-progress chunked snapshot transfers per peer (§7).
    private final Map<String, SnapshotTransfer> snapshotTransfers = new ConcurrentHashMap<>();

    // Follower-side: buffer for reassembling incoming snapshot chunks.
    /** The snapshot this follower is currently receiving, chunk by chunk; null between transfers. */
    private SnapshotReceiver snapshotReceiver;

    // The cluster membership currently in effect (§6): id -> "host:port",
    // including self. Always reflects the *latest* configuration entry in
    // our own log, whether or not that entry has committed yet -- exactly
    // as the paper specifies for both vote-counting and commit-majority
    // decisions, so a pending (uncommitted) reconfiguration takes effect
    // immediately rather than waiting to commit. currentConfigurationIndex
    // is the log index that configuration came from, or 0 if there is no
    // configuration entry yet and we're still running on the bootstrap
    // configuration from the .properties file.
    private volatile Map<String, String> currentConfiguration;
    private volatile long currentConfigurationIndex;

    // §4.2.1 Non-voting learners: id -> "host:port" of every learner in the
    // effective configuration. The leader replicates to these exactly like
    // followers (they live in {@link #peerTransports} too), but they are
    // excluded from every majority decision -- election vote solicitation,
    // commit counting, ReadIndex confirmation and the leader lease -- and are
    // never counted in {@link #majority()}. A learner is promoted to a voting
    // member via {@link #promoteLearner} once it has caught up. Always kept
    // disjoint from {@link #currentConfiguration}.
    private volatile Map<String, String> currentLearners = Map.of();

    // Guards against overlapping background snapshots (§5.1 COW).
    /** How long {@link #shutdown()} waits for a snapshot that is already being written. */
    private static final long SNAPSHOT_SHUTDOWN_WAIT_SECONDS = 60;
    private volatile boolean snapshotInProgress = false;

    // §4.2.3 PreVote: timestamp of the last valid leader contact (heartbeat
    // or AppendEntries). A server only grants a PreVote if it hasn't heard
    // from a leader within the election timeout window.
    //
    // On the monotonic clock ({@link #nowMs()}), like every interval this class measures for
    // an election or a lease: the wall clock can be set, and a step backwards would stretch
    // "recently" by the size of the step. NEVER stands for "no contact yet".
    private volatile long lastLeaderContactMs = NEVER;

    /** Far enough in the past for any window, and far enough from the edge not to overflow. */
    private static final long NEVER = Long.MIN_VALUE / 2;

    /** Milliseconds on the monotonic clock. Good for differences only; it is not a time of day. */
    private static long nowMs() {
        return System.nanoTime() / 1_000_000;
    }

    /**
     * Counts the AppendEntries requests this node has sent. A ReadIndex barrier notes the
     * count when it is created and is confirmed only by answers to requests sent after it.
     * Guarded by {@code lock}.
     */
    private long appendEntriesSent = 0;

    // §3.10: ongoing leadership transfer target, or null if none.
    private volatile String leaderTransferTarget = null;
    private volatile CompletableFuture<Void> leaderTransferResult = null;
    private volatile ScheduledFuture<?> leaderTransferTimeout = null;
    /**
     * Whether TimeoutNow has already gone out for the transfer in flight.
     *
     * <p>checkTransferReadyLocked() runs from every AppendEntries response for the target, and
     * the state it used to test -- leaderTransferTarget -- is only cleared in the outbound
     * call's asynchronous callback. Under load several responses arrive before that callback
     * returns, so each of them sent another TimeoutNow.
     *
     * <p>The target then defeats itself: every TimeoutNow abandons the campaign the previous
     * one started. Measured on the cluster, in four milliseconds -- three sends, three
     * elections, terms 81, 82, 83, none of them finished, and the target ended up a follower
     * voting for the leader it was supposed to replace. It explains why the transfer worked
     * when the cluster was idle and never worked under load: idle means one response.
     */
    private volatile boolean leaderTransferSent = false;

    // Linearizable reads (ReadIndex / Lease): pending read barriers awaiting
    // leadership confirmation from a majority of peers.
    private final List<ReadBarrier> pendingReadBarriers = new ArrayList<>();
    // Lease-based reads: per peer, when the latest AppendEntries it acknowledged was SENT
    // (monotonic clock). Not when the answer arrived: the follower restarted its election
    // timer no later than it answered, and nothing bounds how long the answer then travelled.
    // Counting from the send is the one instant that is certainly not after the follower's.
    private final Map<String, Long> peerLastAckMs = new ConcurrentHashMap<>();

    // Joint consensus (§6): the old configuration during a C_old,new
    // transition, or null when not in joint mode.
    private volatile Map<String, String> oldConfiguration;

    private final Random random = new Random();
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTask;

    // v119 -- staleness guard for the election timer.
    //
    // ScheduledFuture.cancel(false) does NOT stop a task that has already begun
    // running, and a scheduled startElection() that is blocked on lock.lock() has
    // already begun. Every cancel() below is therefore best-effort only: the task
    // it "cancelled" may still be sitting on the lock, and will proceed the moment
    // whoever holds the lock lets go -- by which point the node may have already
    // become CANDIDATE, LEADER, or heard from a live leader.
    //
    // The epoch closes that window. resetElectionTimer() bumps it and hands the new
    // value to the task it schedules; a task whose captured epoch no longer matches
    // is by definition stale and aborts. Anything that legitimately invalidates a
    // pending election -- a timer reset, a step-down, winning an election -- moves
    // the epoch, so no separate bookkeeping is needed at each site.
    private final AtomicLong electionEpoch = new AtomicLong();
    // Guards resetElectionTimer: a node that has not been formally started
    // (i.e. start() has not been called) must not schedule election timers,
    // because it may be receiving RPCs from a leader before being officially
    // admitted to the cluster (e.g. during a §6 addServer sequence in tests
    // where the gRPC server is brought up ahead of start()). Scheduling the
    // timer in that state would cause spurious term inflation that disrupts
    // the ongoing cluster.
    private volatile boolean started = false;
    // Set by start(), consumed by the single resetElectionTimer() call that follows it: the very
    // first election timer of a node's life is scaled by BOOT_DELAY_FACTOR, every later one is an
    // ordinary timeout. A restarting node has to outwait the leader's transport reconnecting to
    // it, not just the leader's heartbeat interval.
    private volatile boolean bootGracePending = false;

    public RaftNode(RaftConfig config,
                     RaftStorage store,
                     StateMachine stateMachine,
                     RaftTransportFactory transportFactory,
                     RaftMetrics metrics) {
        this(config, store, StateMachineRegistry.of(stateMachine), transportFactory, metrics);
    }

    /**
     * A node that drives the application state machine and the named ones beside
     * it (see {@link StateMachineRegistry}). Every server of a cluster must be
     * given the same names.
     */
    public RaftNode(RaftConfig config,
                     RaftStorage store,
                     StateMachineRegistry stateMachines,
                     RaftTransportFactory transportFactory,
                     RaftMetrics metrics) {
        this.config = config;
        this.store = store;
        this.witness = config.isWitness();
        // A witness is handed state machines like any node -- the wiring does not know
        // the difference -- and uses none of them: the whole registry is replaced, so a
        // committed entry for any name is accepted and forgotten.
        this.stateMachines = witness ? StateMachineRegistry.forWitness(new WitnessStateMachine()) : stateMachines;
        this.transportFactory = transportFactory;
        this.metrics = metrics;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "raft-" + config.selfId());
            t.setDaemon(true);
            return t;
        });
        // Snapshots get a thread of their own, and it is not a preference.
        //
        // The scheduler above has two threads and four other users: the election timer,
        // the heartbeat, the read barrier and the leadership-transfer timeout. Every one
        // of those is short and bounded. Taking a snapshot is the only work in this class
        // with no bound at all -- it serialises whatever the state machine holds and
        // writes it to disk -- and it used to run there too.
        //
        // The copy-on-write path (see takeSnapshotAsync) already keeps serialisation off
        // the Raft lock, which makes it easy to believe the cost was contained. It is not
        // the lock that decides elections; it is the timers, and they were sharing two
        // threads with the one task that can occupy them indefinitely. On this repo's
        // demo cluster a snapshot costs 5 ms and nothing is visible -- which is precisely
        // the shape of a fault that waits for a bigger state machine to appear.
        //
        // Single-threaded, not a pool: snapshotInProgress already permits only one at a
        // time, so a second thread could never have work.
        this.snapshotExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "raft-snapshot-" + config.selfId());
            t.setDaemon(true);
            return t;
        });
        // Still single-threaded at this point (inside the constructor), so
        // it's safe to seed our view of the cluster without holding `lock`.
        // If a previous run already took (or installed) a snapshot, restore
        // the state machine from it and fast-forward past everything it
        // covers (§7) before working out our effective configuration --
        // recomputeEffectiveConfiguration() needs the snapshot to already
        // be in place if the configuration entry it would otherwise scan
        // for has since been compacted away.
        RaftStorage.SnapshotMeta existingSnapshot = store.getSnapshotMeta();
        if (existingSnapshot != null) {
            refuseToStartFromAWitnessSnapshot(existingSnapshot);
            restoreStateMachineFromStoredSnapshot();
            clientSessions.restore(existingSnapshot.sessionData);
            lastApplied.set(existingSnapshot.lastIncludedIndex);
            commitIndex.set(Math.max(commitIndex.get(), existingSnapshot.lastIncludedIndex));
        }
        recomputeEffectiveConfiguration();
        metrics.registerGauges(
                store::getCurrentTerm,
                commitIndex::get,
                lastApplied::get,
                () -> role.ordinal(),
                () -> currentConfiguration.size(),
                store::getLastLogIndex,
                store::getSnapshotIndex);
        metrics.registerWitnessGauge(() -> witness ? 1 : 0);
        metrics.registerPendingGauges(pendingClientRequests::size, this::pendingReadCount);
    }

    /** Every server starts out as a follower (§5.1). */
    public void start() {
        started = true;
        bootGracePending = true;
        log("starting as FOLLOWER, currentTerm=" + store.getCurrentTerm()
                + ", lastLogIndex=" + store.getLastLogIndex()
                + ", snapshotIndex=" + store.getSnapshotIndex()
                + ", configuration=" + currentConfiguration.keySet());
        // v122: treat startup as leader contact for stickiness purposes. The field initialises to
        // NEVER, i.e. "never heard from a leader", so a node that had only just come up granted
        // (pre-)votes to anyone before it had any chance to hear from the incumbent. During a
        // rolling restart that is the second grant. This does NOT delay our own campaign -- the
        // election timer is driven by resetElectionTimer() below, not by this field -- it only
        // stops us from unseating a leader we have not even listened for yet.
        lastLeaderContactMs = nowMs();
        resetElectionTimer();
    }

    /**
     * Stops this node. When this returns, the node no longer writes to its storage.
     *
     * <p>That includes a snapshot that was already running: it is not cut off in the
     * middle of its write, it is waited for. Without the wait it finishes some time
     * after this method has returned and compacts the log then -- under whoever
     * opened the same storage in the meantime. A node restarted on that storage had
     * restored the older snapshot and found the entries after it gone.
     */
    public void shutdown() {
        scheduler.shutdownNow();
        snapshotExecutor.shutdownNow();
        awaitSnapshotExecutor();
        for (RaftTransport transport : peerTransports.values()) {
            transport.close();
        }
    }

    private void awaitSnapshotExecutor() {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    if (!snapshotExecutor.awaitTermination(SNAPSHOT_SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
                        log("shutdown: a snapshot is still running after " + SNAPSHOT_SHUTDOWN_WAIT_SECONDS
                                + " s; not waiting any longer");
                    }
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ------------------------------------------------------------------
    // Election timer / heartbeats
    // ------------------------------------------------------------------

    /**
     * A follower (or candidate) that hears nothing valid for a randomized
     * interval gives up waiting and starts an election of its own. The
     * randomization is what keeps split votes rare and lets them resolve
     * quickly when they do happen (§5.2).
     * <p>
     * Does nothing if {@link #start()} has not yet been called: a node
     * that is receiving RPCs before being formally admitted to the cluster
     * (e.g. while a §6 AddServer change is propagating) must not start
     * timing out and kicking off elections prematurely, as that would
     * inflate terms and disrupt the current leader.
     */
    private void resetElectionTimer() {
        if (!started) return;
        if (majority() == 1) {
            startElection();
            return;
        }
        if (electionTimer != null) electionTimer.cancel(false);
        // Invalidate any previously scheduled task, including one that cancel(false)
        // could not stop because it was already running (blocked on `lock`).
        long epoch = electionEpoch.incrementAndGet();
        int timeoutMs = ELECTION_TIMEOUT_MIN_MS
                + random.nextInt(ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS + 1);
        // Issue #2, requester side: the first timer after start() is the one that decides whether
        // a restarting node campaigns before the incumbent's transport has reconnected to it. The
        // measured gap between "starting as FOLLOWER" and the first PreVote was 498-802 ms, i.e.
        // an ordinary timeout expired long before the first heartbeat arrived. Scaling only this
        // one timer buys that window back without slowing down a real failover: from the first
        // reset onwards -- which any heartbeat performs -- the node is on the normal schedule.
        if (witness) {
            // A witness that wins has to hand the lead straight on, which costs a second
            // election. Waiting longer lets a data node that can win do so first; the
            // witness only gets there when none can -- the case it stands for election for.
            timeoutMs *= WITNESS_ELECTION_FACTOR;
        }
        if (bootGracePending) {
            bootGracePending = false;
            timeoutMs *= BOOT_DELAY_FACTOR;
            log("boot grace: first election timer extended to " + timeoutMs + " ms (factor "
                    + BOOT_DELAY_FACTOR + ")");
        }
        lastArmedElectionTimeoutMs = timeoutMs;
        electionTimer = scheduler.schedule(() -> startElectionIfCurrent(epoch), timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Entry point for a <em>scheduled</em> election attempt. Re-validates, after
     * acquiring the lock, that the timer that scheduled this task is still the
     * current one -- see {@link #electionEpoch}. A stale task returns silently:
     * it must not reset the timer either, because a live one already exists.
     */
    // Package-private (not private) so the election tests can drive this seam directly:
    // the scheduler is node-internal, so a stale timer task cannot otherwise be reproduced
    // without a probabilistic race.
    void startElectionIfCurrent(long scheduledEpoch) {
        lock.lock();
        try {
            if (scheduledEpoch != electionEpoch.get()) return;
            startElection();
        } finally {
            lock.unlock();
        }
    }

    /**
     * §4.2.3 PreVote + §5.2: before bumping the term, first check with
     * a majority that they'd grant a vote and haven't heard from a leader
     * recently. Only if the PreVote succeeds does the real election start.
     * Single-node clusters skip PreVote and elect immediately.
     */
    // Package-private for the election tests -- see startElectionIfCurrent.
    void startElection() {
        lock.lock();
        try {
            // v119: a leader has nothing to elect. The epoch guard above already
            // drops a stale timer task, but this states the invariant directly and
            // covers any future caller that does not come through the timer.
            if (role == ServerRole.LEADER) return;
            if (!currentConfiguration.containsKey(config.selfId())) {
                resetElectionTimer();
                return;
            }
            if (majority() == 1) {
                role = ServerRole.CANDIDATE;
                currentLeaderId = null;
                long newTerm = store.getCurrentTerm() + 1;
                store.setTermAndVote(newTerm, config.selfId());
                metrics.electionStarted();
                becomeLeaderLocked();
                return;
            }

            long proposedTerm = store.getCurrentTerm() + 1;
            PreVoteRequest preVoteRequest = PreVoteRequest.newBuilder()
                    .setTerm(proposedTerm)
                    .setCandidateId(config.selfId())
                    .setLastLogIndex(store.getLastLogIndex())
                    .setLastLogTerm(store.getLastLogTerm())
                    .build();

            // Who granted, not how many: during a configuration change the question is
            // whether a majority of C_old AND a majority of C_new did (see Quorum), and no
            // count over the union can answer that.
            java.util.Set<String> preVotesGranted = ConcurrentHashMap.newKeySet();
            preVotesGranted.add(config.selfId());
            // Issue #3: one latch per round, so the quorum threshold converts into an election
            // exactly once no matter how many grants cross it. Scoped to the round rather than
            // to the node -- a later round must be free to decide again.
            AtomicBoolean roundDecided = new AtomicBoolean(false);
            try {
                // §4.2.1: solicit (Pre)Votes only from voting members -- never
                // from learners, which do not participate in elections.
                for (String peerId : votingPeerIdsExcludingSelf()) {
                    RaftTransport peer = peerTransports.get(peerId);
                    if (peer == null) continue;
                    peer.preVote(preVoteRequest).whenComplete((response, t) -> {
                        if (t == null) {
                            handlePreVoteResponse(proposedTerm, peerId, response, preVotesGranted, roundDecided);
                        } else {
                            log("PreVote RPC failed", t);
                        }
                    });
                }
            } catch (RuntimeException e) {
                // A SYNCHRONOUS failure here (e.g. lazy peer-channel/TLS-context construction
                // throwing, rather than the RPC itself failing asynchronously above) used to
                // propagate straight out of this method — which is the Runnable passed to
                // scheduler.schedule(). ScheduledExecutorService swallows an uncaught exception
                // from a scheduled task completely silently: no log, no retry, nothing. The
                // node would log its one "starting as FOLLOWER" line and then go dark forever,
                // since resetElectionTimer() below — the only thing that schedules the NEXT
                // election attempt — would never run either. Confirmed the hard way: a live
                // 3-node cluster went silent immediately after boot with zero further raft
                // logging, even at DEBUG, until this was found by reading the source directly.
                log("startElection() failed to reach peers, will retry: " + e);
            }
            resetElectionTimer();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Counts one PreVote reply and, when the round reaches quorum, converts it into a real
     * election -- exactly once per round (issue #3).
     *
     * <p>The latch is what makes "exactly once" true by construction rather than by consequence.
     * The two term comparisons here and in {@link #startRealElection} also happen to swallow a
     * late second grant, but only because the first one moved {@code currentTerm}: that is a
     * side effect of another guard, not a statement of this rule, and it stops holding the
     * moment a caller reaches quorum without bumping the term. Without the latch the node
     * reproduced the two-elections-in-52-ms shape from the issue -- a check-then-act with no
     * atomic transition, where every grant that crossed the threshold campaigned for whatever
     * term was next when it happened to arrive.
     */
    private void handlePreVoteResponse(long proposedTerm, String peerId, PreVoteResponse response,
                                       java.util.Set<String> preVotesGranted, AtomicBoolean roundDecided) {
        lock.lock();
        try {
            if (store.getCurrentTerm() + 1 != proposedTerm) {
                return;
            }
            if (!response.getVoteGranted()) {
                return;
            }
            preVotesGranted.add(peerId);
            if (!Quorum.reached(preVotesGranted, currentConfiguration, oldConfiguration)) {
                return;
            }
            // The round is consumed here, not by whatever startRealElection does with it.
            if (!roundDecided.compareAndSet(false, true)) return;
            startRealElection(proposedTerm);
        } finally {
            lock.unlock();
        }
    }

    private void startRealElection(long newTerm) {
        startRealElection(newTerm, false);
    }

    /**
     * @param leadershipTransfer true only when the incumbent leader told us to campaign via
     *        TimeoutNow (§3.10). Marks the outgoing RequestVote RPCs so voters skip their
     *        stickiness check -- see {@link #handleRequestVote}.
     */
    private void startRealElection(long newTerm, boolean leadershipTransfer) {
        // v119: two independent ways in, both of which can arrive late.
        //
        //  * role == LEADER -- we already won a term; bumping it now would discard
        //    our own no-op and every client entry accepted since, all uncommitted,
        //    for nothing.
        //  * newTerm <= currentTerm -- a grant from a PreVote round that has already
        //    been decided (or superseded). Since proposedTerm is always
        //    currentTerm + 1 at the time the round starts, this single comparison
        //    marks the round consumed: the first grant to cross the threshold moves
        //    currentTerm to newTerm, and every later grant of that round fails here.
        //    No separate round-ID bookkeeping is needed.
        if (role == ServerRole.LEADER || newTerm <= store.getCurrentTerm()) return;
        role = ServerRole.CANDIDATE;
        currentLeaderId = null;
        store.setTermAndVote(newTerm, config.selfId());
        metrics.electionStarted();
        log("PreVote succeeded -> starting election for term " + newTerm);

        resetElectionTimer();

        RequestVoteRequest request = RequestVoteRequest.newBuilder()
                .setTerm(newTerm)
                .setCandidateId(config.selfId())
                .setLastLogIndex(store.getLastLogIndex())
                .setLastLogTerm(store.getLastLogTerm())
                .setLeadershipTransfer(leadershipTransfer)
                .build();

        java.util.Set<String> votesGranted = ConcurrentHashMap.newKeySet();
        votesGranted.add(config.selfId());
        // §4.2.1: request votes only from voting members, never from learners.
        for (String peerId : votingPeerIdsExcludingSelf()) {
            RaftTransport peer = peerTransports.get(peerId);
            if (peer == null) continue;
            peer.requestVote(request).whenComplete((response, t) -> {
                if (t == null) handleRequestVoteResponse(newTerm, peerId, response, votesGranted);
            });
        }
    }

    private void handleRequestVoteResponse(long electionTerm, String peerId, RequestVoteResponse response,
                                           java.util.Set<String> votesGranted) {
        lock.lock();
        try {
            if (response.getTerm() > store.getCurrentTerm()) {
                becomeFollowerLocked(response.getTerm());
                return;
            }
            if (role != ServerRole.CANDIDATE || store.getCurrentTerm() != electionTerm) {
                return; // stale response from an election we've since left
            }
            if (response.getVoteGranted()) {
                votesGranted.add(peerId);
                // §6: a majority of C_new and, while a change is in flight, of C_old as well.
                // This compared a count over the union against the larger of the two
                // majorities, which three servers of C_new satisfy with no vote from C_old.
                if (Quorum.reached(votesGranted, currentConfiguration, oldConfiguration)) {
                    becomeLeaderLocked();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Becoming leader right away sends an empty AppendEntries (heartbeat)
     * to every server, before any of them can time out and start a
     * competing election (§5.2).
     */
    private void becomeLeaderLocked() {
        role = ServerRole.LEADER;
        currentLeaderId = config.selfId();
        metrics.electionWon();
        log("elected LEADER for term " + store.getCurrentTerm());
        // v119: cancel(false) cannot stop an election task that is already running
        // and merely waiting for this lock. Moving the epoch makes such a task abort
        // when it finally gets in. Without this, a task scheduled a moment before we
        // won would proceed to run a full PreVote round for term+1 and unseat us.
        electionEpoch.incrementAndGet();
        if (electionTimer != null) electionTimer.cancel(false);
        if (heartbeatTask != null) heartbeatTask.cancel(false);
        // Acknowledgements from an earlier term of ours say nothing about this one.
        peerLastAckMs.clear();

        // §10.2.1: everything already in our log was written durably as a
        // follower/candidate, so our own disk match starts there.
        leaderDiskMatchIndex.set(store.getLastLogIndex());

        // §8: commit a blank no-op entry for our new term right away. Until
        // an entry from our own term has committed, we can't be sure which
        // older entries are actually committed yet, even though Leader
        // Completeness guarantees we already have them in our log.
        long noOpIndex = store.getLastLogIndex() + 1;
        leaderNoOpIndex = noOpIndex;
        long leaderTerm = store.getCurrentTerm();
        LogEntry noOp = LogEntry.newBuilder()
                .setIndex(noOpIndex)
                .setTerm(leaderTerm)
                .setCommand(ByteString.EMPTY)
                .build();
        store.appendEntriesDeferSync(List.of(noOp)).thenRun(() ->
                onLeaderDiskSyncComplete(noOpIndex, leaderTerm));

        long lastLogIndex = store.getLastLogIndex();
        for (String peerId : peerTransports.keySet()) {
            nextIndex.put(peerId, lastLogIndex + 1);
            matchIndex.put(peerId, 0L);
        }

        heartbeatTask = scheduler.scheduleWithFixedDelay(
                this::sendHeartbeats, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    private void sendHeartbeats() {
        lock.lock();
        try {
            if (role != ServerRole.LEADER) return;
            for (String peerId : peerTransports.keySet()) {
                replicateTo(peerId);
            }
            handWitnessLeadershipOnLocked();
            logReplicationStatus();
        } finally {
            lock.unlock();
        }
    }

    /**
     * v101 — every {@value #REPLICATION_STATUS_INTERVAL_MS} ms the leader states, for each
     * member it is responsible for, how far it has actually got. This is the line that was
     * missing when a follower sat at a stale index for hours: from the outside the cluster
     * looked healthy (writes committed on the remaining majority) while a third of the nodes
     * silently received nothing.
     *
     * <p>Members with a transport are listed with match/next index; members of the
     * configuration WITHOUT a transport are called out separately — that is a defect, not a
     * lag. A peer whose matchIndex trails the leader's last log index is flagged.
     */
    private void logReplicationStatus() {
        long now = System.currentTimeMillis();
        if (now - lastReplicationStatusLogMs < REPLICATION_STATUS_INTERVAL_MS) return;
        lastReplicationStatusLogMs = now;

        long lastLogIndex = store.getLastLogIndex();
        StringBuilder sb = new StringBuilder("replication: lastLogIndex=").append(lastLogIndex)
                .append(" commitIndex=").append(commitIndex.get());

        java.util.Set<String> published = new java.util.LinkedHashSet<>();
        for (Map.Entry<String, String> member : currentConfiguration.entrySet()) {
            String id = member.getKey();
            if (id.equals(config.selfId())) continue;
            appendPeerStatus(sb, id, lastLogIndex, "voter");
            publishPeerMetrics(id, "voter");
            published.add(id);
        }
        for (Map.Entry<String, String> learner : currentLearners.entrySet()) {
            appendPeerStatus(sb, learner.getKey(), lastLogIndex, "learner");
            publishPeerMetrics(learner.getKey(), "learner");
            published.add(learner.getKey());
        }
        // Everything not just published has left the configuration. Done here rather than
        // where nodes are removed, so no removal path can forget it and leave a series
        // claiming the leader still replicates to a node it does not.
        metrics.forgetPeersExcept(published);
        log(sb.toString());
    }

    /**
     * The same per-peer facts the line above prints, as metrics.
     *
     * <p>The log line has been the only way to see the live configuration from outside,
     * and a log line is not an interface -- reading it means an SSH session and a parser
     * coupled to a format nobody declared stable. The series carry the peer id and its
     * role, so on a leader the set of series is the configuration itself.
     */
    private void publishPeerMetrics(String peerId, String role) {
        if (!peerTransports.containsKey(peerId)) {
            // No transport means this node has never been replicated to -- a defect the
            // line above calls out. Publishing a match index for it would dress that up
            // as mere lag.
            return;
        }
        Long lastAck = peerLastAckMs.get(peerId);
        metrics.peerReplication(peerId, role,
                matchIndex.getOrDefault(peerId, 0L),
                lastAck == null ? -1L : nowMs() - lastAck);
    }

    private void appendPeerStatus(StringBuilder sb, String id, long lastLogIndex, String role) {
        sb.append(" | ").append(id).append('(').append(role).append(')');
        if (!peerTransports.containsKey(id)) {
            sb.append(" NO-TRANSPORT!");     // never replicated to — a defect
            return;
        }
        long match = matchIndex.getOrDefault(id, 0L);
        long next = nextIndex.getOrDefault(id, 1L);
        int inflight = peerInflight.getOrDefault(id, 0);
        Long lastAck = peerLastAckMs.get(id);
        sb.append(" match=").append(match)
          .append(" next=").append(next)
          .append(" inflight=").append(inflight)
          .append(" lastAck=").append(lastAck == null ? "never"
                  : (nowMs() - lastAck) + "ms");
        if (match < lastLogIndex) sb.append(" LAGGING");
        String failure = peerLastFailure.get(id);
        if (failure != null) sb.append(" lastError=").append(failure);
    }

    /** One line per peer per {@value #FAILURE_LOG_THROTTLE_MS} ms — enough to see it, not a flood. */
    private void logThrottled(String peerId, String message) {
        long now = System.currentTimeMillis();
        Long last = peerLastFailureLogMs.get(peerId);
        if (last != null && now - last < FAILURE_LOG_THROTTLE_MS) return;
        peerLastFailureLogMs.put(peerId, now);
        logWarn(peerId + ": " + message);
    }

    /**
     * Steps down to follower, whether because we just learned of a higher
     * term, some other server has already established itself as leader for
     * our current term (§5.2), or we just finished committing a
     * configuration that removed us from the cluster (§6).
     */
    private void becomeFollowerLocked(long newTerm) {
        ServerRole previousRole = role;
        long previousTerm = store.getCurrentTerm();
        if (newTerm > store.getCurrentTerm()) {
            store.setTermAndVote(newTerm, null);
        }
        if (role == ServerRole.LEADER && heartbeatTask != null) {
            heartbeatTask.cancel(false);
        }
        leaderTransferTarget = null;
        leaderTransferResult = null;
        leaderTransferSent = false;
        if (leaderTransferTimeout != null) {
            leaderTransferTimeout.cancel(false);
            leaderTransferTimeout = null;
        }
        for (ReadBarrier rb : pendingReadBarriers) {
            rb.future.completeExceptionally(new NotLeaderException(null));
        }
        pendingReadBarriers.clear();
        failUncommittedClientRequestsLocked();
        // oldConfiguration is NOT cleared here. It mirrors the latest configuration entry in the
        // log, and stepping down does not change the log: a server that is in C_old,new stays in
        // it, as a follower and as a candidate, until an entry says otherwise. Clearing it here
        // let a node that had merely seen a higher term campaign on a majority of C_new alone.
        metrics.stepDown();
        // v119: this used to be entirely silent -- only metrics.stepDown() recorded it.
        // A step-down is the single most load-bearing event when reading an election
        // trace, and its absence from the log was read (in issue B) as proof that no
        // step-down had happened. Absence of a log line was never evidence of absence.
        if (previousRole != ServerRole.FOLLOWER || newTerm > previousTerm) {
            log("stepping down: " + previousRole + " term " + previousTerm
                    + " -> FOLLOWER term " + store.getCurrentTerm());
        }
        role = ServerRole.FOLLOWER;
        resetElectionTimer();
    }

    /**
     * Fails every client request whose entry has not committed. Called when this node stops
     * leading. Caller must hold {@code lock}.
     *
     * <p>The requests are keyed by log index, and after a step-down the index no longer
     * identifies the entry: the next leader may overwrite ours with one of its own, and when
     * that one is applied here the waiting client would be handed its result -- told that a
     * write succeeded which no longer exists. So the answer from here is the honest one, "not
     * the leader any more, outcome unknown": the entry may still commit under the next leader,
     * and a client that needs to know resubmits with its session (§8), which tells a repeat
     * from a first application.
     *
     * <p>Entries at or below commitIndex are left alone. They are committed, they are ours,
     * and the apply loop that may be running further up this very call stack answers them.
     */
    private void failUncommittedClientRequestsLocked() {
        if (pendingClientRequests.isEmpty()) return;
        long committed = commitIndex.get();
        pendingClientRequests.entrySet().removeIf(pending -> {
            if (pending.getKey() <= committed) return false;
            pending.getValue().completeExceptionally(new NotLeaderException(null));
            return true;
        });
    }

    // ------------------------------------------------------------------
    // RequestVote RPC (Figure 2)
    // ------------------------------------------------------------------

    /**
     * RequestVote receiver logic: grants at most one vote per term, and
     * only to a candidate whose log is at least as up to date as our own
     * (§5.2, §5.4.1).
     */
    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        lock.lock();
        try {
            long currentTerm = store.getCurrentTerm();

            // v125 (§4.2.3): while we still hear from a leader, ignore the request outright --
            // BEFORE adopting its term. Denying the vote but adopting the term would be no
            // protection at all: the term bump alone deposes our leader and forces a fresh
            // election, which is the disruption stickiness exists to prevent. The PreVote handler
            // has had this guard since the beginning; RequestVote never did, so a candidate that
            // skips PreVote (or an old/hostile peer) could still unseat a healthy leader.
            //
            // The exception is a leadership transfer: there the incumbent itself asked the
            // candidate to campaign, so every voter's "I still hear a leader" is true and
            // irrelevant. Without this carve-out, adding the guard would break transferLeadership.
            if (!request.getLeadershipTransfer() && hasLeaderStickiness()) {
                log("denied vote to " + request.getCandidateId() + " for term " + request.getTerm()
                        + " (stickiness: leader " + currentLeaderId + " still active"
                        + ", sinceLeaderContact=" + (role == ServerRole.LEADER
                                ? "self" : (nowMs() - lastLeaderContactMs) + "ms")
                        + "), term not adopted");
                return RequestVoteResponse.newBuilder()
                        .setTerm(currentTerm).setVoteGranted(false).build();
            }

            if (request.getTerm() > currentTerm) {
                becomeFollowerLocked(request.getTerm());
                currentTerm = request.getTerm();
            }

            if (request.getTerm() < currentTerm) {
                return RequestVoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
            }

            String votedFor = store.getVotedFor();
            boolean canVote = (votedFor == null || votedFor.equals(request.getCandidateId()));
            boolean logOk = isLogAtLeastAsUpToDate(request.getLastLogIndex(), request.getLastLogTerm());

            if (canVote && logOk) {
                store.setTermAndVote(currentTerm, request.getCandidateId());
                resetElectionTimer();
                metrics.voteGranted();
                log("granted vote to " + request.getCandidateId() + " for term " + currentTerm);
                return RequestVoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(true).build();
            }
            // Say why. This return was silent, and on 2026-11-25 that silence cost an hour on
            // a cluster where a leadership transfer kept failing: the target campaigned, every
            // voter adopted its term, nobody granted it anything, and no node anywhere recorded
            // a reason. The PreVote handler learned this in v122 (issue #2) -- "the one side
            // that knows the reason was the silent one" -- and RequestVote never did.
            log("denied vote to " + request.getCandidateId() + " for term " + currentTerm
                    + " (canVote=" + canVote + ", logOk=" + logOk
                    + ", votedFor=" + votedFor
                    + ", mine=" + store.getLastLogTerm() + "/" + store.getLastLogIndex()
                    + ", theirs=" + request.getLastLogTerm() + "/" + request.getLastLogIndex()
                    + ", transfer=" + request.getLeadershipTransfer() + ")");
            return RequestVoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
        } finally {
            lock.unlock();
        }
    }

    /**
     * §5.4.1: compares the (term, index) of each log's last entry -- the
     * log with the later term wins; if the terms tie, the longer log wins.
     */
    private boolean isLogAtLeastAsUpToDate(long otherLastIndex, long otherLastTerm) {
        long myLastTerm = store.getLastLogTerm();
        long myLastIndex = store.getLastLogIndex();
        if (otherLastTerm != myLastTerm) return otherLastTerm > myLastTerm;
        return otherLastIndex >= myLastIndex;
    }

    // ------------------------------------------------------------------
    // PreVote RPC (§4.2.3 / §9.6) + Leader Stickiness
    // ------------------------------------------------------------------

    /**
     * Leader stickiness: returns true if this server has heard from a valid
     * leader recently enough that it should refuse to grant (Pre)Votes.
     * This prevents a candidate from disrupting a functioning cluster --
     * especially a partitioned node whose term inflated while isolated.
     *
     * <p>v122: a LEADER counts as having contact -- with itself. {@code lastLeaderContactMs} is
     * only ever written by {@link #handleAppendEntries}, which a leader never receives, so its
     * value is frozen at whatever it was before this node won its term: seconds, or days, old.
     * Stickiness was therefore permanently false on exactly the one node whose refusal matters
     * most. With three voters, quorum is two and the candidate counts itself, so the leader's
     * grant alone was enough to unseat it -- which is why every restart of any voter took the
     * leadership (issue #2), deterministically rather than by chance.
     */
    private boolean hasLeaderStickiness() {
        // Without this line a LEADER fell through to the timestamp below -- which
        // handleAppendEntries never writes on a leader, so it read as "no leader contact since
        // 1970" and the incumbent granted the vote that unseated it. That was issue #2, and it
        // was the half of the defect that decided whether the restarting node won.
        if (role == ServerRole.LEADER) return true;
        return nowMs() - lastLeaderContactMs < ELECTION_TIMEOUT_MIN_MS;
    }

    @Override
    public PreVoteResponse handlePreVote(PreVoteRequest request) {
        lock.lock();
        try {
            long currentTerm = store.getCurrentTerm();

            if (request.getTerm() < currentTerm) {
                return PreVoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(false).build();
            }

            boolean logOk = isLogAtLeastAsUpToDate(request.getLastLogIndex(), request.getLastLogTerm());
            boolean sticky = hasLeaderStickiness();
            boolean grant = logOk && !sticky;
            // v122: say who asked, what we answered, and why. Issue #2 could name the granting
            // peer only by elimination, because the responder logged nothing at all -- the one
            // side that knows the reason was the silent one.
            log((grant ? "granted" : "denied") + " pre-vote to " + request.getCandidateId()
                    + " for term " + request.getTerm()
                    + " (logOk=" + logOk + ", stickiness=" + sticky
                    + ", role=" + role
                    + ", sinceLeaderContact=" + (role == ServerRole.LEADER
                            ? "self" : (nowMs() - lastLeaderContactMs) + "ms")
                    + ")");
            return PreVoteResponse.newBuilder().setTerm(currentTerm).setVoteGranted(grant).build();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // AppendEntries RPC (Figure 2)
    // ------------------------------------------------------------------

    /**
     * AppendEntries receiver logic (§5.3): rejects stale or mismatched
     * requests, resolves conflicts in favor of the leader's log, appends
     * whatever's new (tracking configuration entries as it goes, §6), and
     * advances commitIndex when the leader says it's safe to.
     */
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        lock.lock();
        try {
            long currentTerm = store.getCurrentTerm();

            // Rule 1: a leader from an earlier term is stale; reject it.
            if (request.getTerm() < currentTerm) {
                return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(false)
                        .setConflictLastLogIndex(store.getLastLogIndex()).build();
            }

            // A request with term >= currentTerm proves a legitimate leader
            // is active, so we recognize it and become/stay a follower,
            // resetting our own timer either way.
            if (request.getTerm() > currentTerm || role != ServerRole.FOLLOWER) {
                becomeFollowerLocked(request.getTerm());
            }
            currentTerm = store.getCurrentTerm();
            currentLeaderId = request.getLeaderId();
            resetElectionTimer();
            // NOTE (v105): lastLeaderContactMs is deliberately NOT set here. It used to be — and
            // that made "leader contact" mean "bytes arrived", not "AppendEntries was actually
            // processed". A node that threw an exception on every AppendEntries (the truncateFrom
            // bug, v104) therefore reported healthy readiness while replicating nothing at all,
            // for hours. Contact is now recorded only on the paths that produce a response — the
            // consistency-check rejection below, and the successful append at the end.

            // Rule 2: our log must actually have an entry at prevLogIndex
            // whose term matches, or we can't safely accept what follows it.
            // getTermAt also resolves correctly if prevLogIndex happens to
            // be exactly our snapshot boundary (§7), where the entry itself
            // no longer physically exists but its term is still known.
            if (request.getPrevLogIndex() > 0) {
                long prevTerm = store.getTermAt(request.getPrevLogIndex());
                if (prevTerm < 0 || prevTerm != request.getPrevLogTerm()) {
                    // A rejection IS healthy contact: we answered, and the leader will back off
                    // and retry from further back (§5.3).
                    lastLeaderContactMs = nowMs();
                    // v118: tell the leader how far our log actually reaches. A wiped peer reports 0,
                    // so the leader can drop nextIndex to 1 (<= snapshotIndex) and switch to
                    // InstallSnapshot in a single round instead of pinning at a stale matchIndex.
                    return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(false)
                            .setConflictLastLogIndex(store.getLastLogIndex()).build();
                }
            }

            // Rules 3 & 4: overwrite anything that conflicts with the
            // leader's version, then append whatever we don't already have.
            // If a truncation reaches back far enough to discard the entry
            // our cached configuration came from, we'll need to recompute
            // it afterwards -- unless one of the newly appended entries
            // supersedes it directly first.
            long snapshotIndex = store.getSnapshotIndex();
            long lastNewIndex = request.getPrevLogIndex();
            List<LogEntry> toAppend = new ArrayList<>();
            boolean configurationMayBeStale = false;
            for (LogEntry entry : request.getEntriesList()) {
                if (entry.getIndex() <= snapshotIndex) {
                    // Already folded into our snapshot and therefore
                    // already committed -- by the log matching property
                    // this must agree with what the leader is offering, so
                    // there's nothing left to check or store here.
                    lastNewIndex = entry.getIndex();
                    continue;
                }
                LogEntry existing = store.getLogEntry(entry.getIndex());
                if (existing != null && existing.getTerm() != entry.getTerm()) {
                    store.truncateFrom(entry.getIndex());
                    existing = null;
                    if (entry.getIndex() <= currentConfigurationIndex) {
                        configurationMayBeStale = true;
                    }
                }
                if (existing == null) {
                    toAppend.add(entry);
                }
                lastNewIndex = entry.getIndex();
            }
            if (!toAppend.isEmpty()) {
                store.appendEntries(toAppend);
                for (LogEntry entry : toAppend) {
                    if (entry.hasConfiguration()) {
                        applyConfigurationLocked(entry.getIndex(), entry.getConfiguration());
                        configurationMayBeStale = false; // superseded directly, no rescan needed
                    }
                }
            }
            if (configurationMayBeStale) {
                recomputeEffectiveConfiguration();
            }

            // Rule 5: pull commitIndex forward to whatever the leader says
            // is safe, capped at the last entry we actually just stored.
            //
            // v116: record what the leader says is committed BEFORE applying. During a chunked
            // catch-up this batch may carry only part of what the leader has committed, so
            // commitIndex below is capped at lastNewIndex while leaderCommit runs ahead.
            // maybeTakeSnapshotLocked() (reached from applyCommittedEntries) gates on
            // appliedIndex() >= leaderCommitSeen() to refuse compaction mid-catch-up -- but only
            // if leaderCommitSeen already reflects the leader's true commit. Left below the apply
            // call, leaderCommitSeen() would still read the capped commitIndex during a partial
            // batch and the gate would be blind. This assignment is monotonic (max), so recording
            // it a few statements earlier changes nothing an outside observer can see.
            leaderCommitSeen = Math.max(leaderCommitSeen, request.getLeaderCommit());
            //
            // Only ever forwards. lastNewIndex is where THIS request ends, and a request can
            // end below what we have already committed: a leader that lost track of us starts
            // again from the beginning of its log, and its first batch then stops short of our
            // commitIndex while its leaderCommit lies beyond it.
            long newCommitIndex = Math.min(request.getLeaderCommit(), lastNewIndex);
            if (newCommitIndex > commitIndex.get()) {
                commitIndex.set(newCommitIndex);
                applyCommittedEntries();
            }

            lastLeaderContactMs = nowMs();
            return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(true).build();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // InstallSnapshot RPC (§7)
    // ------------------------------------------------------------------

    /**
     * InstallSnapshot receiver logic (§7, Figure 13): the leader splits its
     * snapshot into chunks and sends one per RPC. Each chunk carries an
     * {@code offset} and a {@code done} flag. The follower buffers incoming
     * chunks; when the final chunk arrives ({@code done=true}) it
     * reassembles the full snapshot, restores the state machine, and
     * discards every log entry at or before {@code lastIncludedIndex}.
     */
    public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        lock.lock();
        try {
            long currentTerm = store.getCurrentTerm();

            if (request.getTerm() < currentTerm) {
                return InstallSnapshotResponse.newBuilder().setTerm(currentTerm).build();
            }
            if (request.getTerm() > currentTerm || role != ServerRole.FOLLOWER) {
                becomeFollowerLocked(request.getTerm());
            }
            currentTerm = store.getCurrentTerm();
            currentLeaderId = request.getLeaderId();
            lastLeaderContactMs = nowMs();
            resetElectionTimer();

            if (request.getLastIncludedIndex() <= store.getSnapshotIndex()) {
                return InstallSnapshotResponse.newBuilder().setTerm(currentTerm).build();
            }
            // A snapshot that ends at or below what we have applied has nothing for us, and
            // installing it would be worse than useless: the state machine would be replaced by
            // the state as of lastIncludedIndex while lastApplied stayed where it is, so the
            // entries in between would be gone from the state and never applied again. Being
            // behind our own snapshot (the check above) is not the only way to be covered --
            // a leader sends a snapshot whenever its nextIndex for us falls to its snapshot
            // boundary, which one failed AppendEntries right after its election is enough for
            // (matchIndex is still 0 then), however current we are. Everything up to
            // lastApplied is committed, so by Log Matching we hold what the snapshot holds;
            // acknowledging lets the leader move on to the entries after it.
            if (request.getLastIncludedIndex() <= lastApplied.get()) {
                discardSnapshotReceiverLocked();
                return InstallSnapshotResponse.newBuilder().setTerm(currentTerm).build();
            }

            // Start afresh on offset 0 or when the snapshot identity changes.
            if (request.getOffset() == 0
                    || snapshotReceiver == null
                    || snapshotReceiver.lastIncludedIndex != request.getLastIncludedIndex()) {
                discardSnapshotReceiverLocked();
                snapshotReceiver = new SnapshotReceiver(
                        request.getLastIncludedIndex(), request.getLastIncludedTerm());
            }

            if (request.getOffset() != snapshotReceiver.expectedOffset) {
                discardSnapshotReceiverLocked();
                return InstallSnapshotResponse.newBuilder().setTerm(currentTerm).build();
            }

            SnapshotReceiver receiver = snapshotReceiver;
            try {
                // Straight to the storage backend's file. The only thing buffered is the
                // few bytes of configuration and session table that follow the payload.
                receiver.accept(request.getData());
                metrics.snapshotChunkReceived();

                if (request.getDone()) {
                    snapshotReceiver = null;
                    byte[] sessionData = receiver.commit();
                    restoreStateMachineFromStoredSnapshot();
                    // The table belongs to the same index as the state it sits beside.
                    // Keeping our own would pair the leader's state with our older idea of
                    // which commands produced it.
                    clientSessions.restore(sessionData);

                    if (receiver.lastIncludedIndex > commitIndex.get()) {
                        commitIndex.set(receiver.lastIncludedIndex);
                    }
                    lastApplied.set(Math.max(lastApplied.get(), receiver.lastIncludedIndex));
                    recomputeEffectiveConfiguration();

                    metrics.snapshotInstalled();
                    log("installed snapshot through index " + receiver.lastIncludedIndex
                            + " (term " + receiver.lastIncludedTerm + ") from " + request.getLeaderId());
                }
            } catch (IOException e) {
                // Nothing was committed: the stored snapshot and the log are as they were.
                // Failing the RPC makes the leader send this chunk again, which no longer
                // matches and so restarts the transfer from the beginning.
                discardSnapshotReceiverLocked();
                receiver.close();
                throw new UncheckedIOException("could not store snapshot chunk at offset "
                        + request.getOffset() + " of snapshot " + request.getLastIncludedIndex(), e);
            }

            return InstallSnapshotResponse.newBuilder().setTerm(currentTerm).build();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // Leader: replication
    // ------------------------------------------------------------------

    /**
     * Sends a follower whatever log entries it's missing, or an empty
     * AppendEntries as a plain heartbeat if it's already caught up (§5.3).
     * This is also how a brand-new member added via {@link #addServer}
     * catches up: it starts with an empty log, so the very first
     * AppendEntries it receives offers it the entire log from index 1.
     */
    private void replicateTo(String peerId) {
        RaftTransport transport = peerTransports.get(peerId);
        if (transport == null) {
            // A configured member with no transport is replicated to by NOBODY — it will
            // never receive an entry again. That must not be silent (v101).
            logThrottled(peerId, "no transport for this peer — it is NOT being replicated to");
            return;
        }
        long peerNextIndex = nextIndex.getOrDefault(peerId, 1L);
        long snapshotIndex = store.getSnapshotIndex();
        if (peerNextIndex <= snapshotIndex) {
            sendInstallSnapshot(peerId, transport);
            return;
        }

        // §10.2.2: skip if we've already hit the pipelining limit.
        int inflight = peerInflight.getOrDefault(peerId, 0);
        if (inflight >= MAX_INFLIGHT_APPENDS) {
            return;
        }

        long currentTerm = store.getCurrentTerm();
        long sentAtMs = nowMs();
        long sequence = ++appendEntriesSent;
        long prevLogIndex = peerNextIndex - 1;
        long prevLogTerm = Math.max(0, store.getTermAt(prevLogIndex));

        // §10.2.2: collect entries up to MAX_BATCH_BYTES so a single
        // large batch doesn't starve heartbeats.
        long lastLogIndex = store.getLastLogIndex();
        List<LogEntry> entries = new ArrayList<>();
        int batchBytes = 0;
        for (long i = peerNextIndex; i <= lastLogIndex; i++) {
            LogEntry entry = store.getLogEntry(i);
            int entrySize = entry.getSerializedSize();
            if (!entries.isEmpty() && batchBytes + entrySize > MAX_BATCH_BYTES) {
                break;
            }
            entries.add(entry);
            batchBytes += entrySize;
        }
        if (!entries.isEmpty()) {
            metrics.replicationSent(entries.size());
        }

        AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                .setTerm(currentTerm)
                .setLeaderId(config.selfId())
                .setPrevLogIndex(prevLogIndex)
                .setPrevLogTerm(prevLogTerm)
                .addAllEntries(entries)
                .setLeaderCommit(commitIndex.get())
                .build();

        long lastSentIndex = entries.isEmpty() ? prevLogIndex : entries.get(entries.size() - 1).getIndex();

        // §10.2.2: optimistically advance nextIndex for pipelining so
        // the next replicateTo call can send subsequent entries without
        // waiting for this RPC's acknowledgment.
        if (!entries.isEmpty()) {
            nextIndex.put(peerId, lastSentIndex + 1);
        }
        peerInflight.merge(peerId, 1, Integer::sum);

        transport.appendEntries(request).whenComplete((response, t) -> {
            if (t != null) {
                lock.lock();
                try {
                    peerInflight.merge(peerId, -1, Integer::sum);
                    long confirmed = matchIndex.getOrDefault(peerId, 0L);
                    nextIndex.put(peerId, Math.max(1, confirmed + 1));
                    // v117: after enough consecutive transport failures, the cached channel is
                    // presumed stuck (peer recreated, connection dead but not self-healing).
                    // Discard it and build a fresh one — a new channel re-resolves DNS and
                    // reconnects, which is what actually un-sticks a peer left at a stale index.
                    int fails = peerConsecutiveFailures.merge(peerId, 1, Integer::sum);
                    if (fails >= MAX_CONSECUTIVE_FAILURES_BEFORE_REBUILD
                            && System.currentTimeMillis()
                                - peerLastRebuildMs.getOrDefault(peerId, 0L) >= REBUILD_COOLDOWN_MS) {
                        peerLastRebuildMs.put(peerId, System.currentTimeMillis());
                        rebuildTransportLocked(peerId);
                    }
                } finally {
                    lock.unlock();
                }
                metrics.replicationFailure();
                // v101: a replication failure used to vanish here without a trace.
                peerLastFailure.put(peerId, String.valueOf(t));
                logThrottled(peerId, "AppendEntries fehlgeschlagen (prevLogIndex=" + prevLogIndex
                        + ", entries=" + entries.size() + "): " + t);
            } else {
                peerConsecutiveFailures.put(peerId, 0);   // the transport is healthy again
                peerLastRebuildMs.remove(peerId);
                handleAppendEntriesResponse(peerId, currentTerm, prevLogIndex, lastSentIndex,
                        sentAtMs, sequence, response);
            }
        });
    }

    /**
     * Discards a peer's cached transport and builds a fresh one (§v117). A new channel performs a
     * fresh DNS resolution and a fresh connection, healing a channel stuck against a peer that has
     * been recreated. Caller must hold {@code lock}. No-op if the peer is no longer a replication
     * target (its address is gone from every configuration map).
     */
    private void rebuildTransportLocked(String peerId) {
        String address = addressOf(peerId);
        if (address == null) return;
        RaftTransport old = peerTransports.remove(peerId);
        if (old != null) {
            try { old.close(); } catch (RuntimeException ignored) { /* best effort */ }
        }
        peerTransports.put(peerId, transportFactory.connect(address));
        peerConsecutiveFailures.put(peerId, 0);
        log("Peer-Transport zu " + peerId + " (" + address + ") nach "
                + MAX_CONSECUTIVE_FAILURES_BEFORE_REBUILD + " Fehlern in Folge neu aufgebaut");
    }

    /** The current "host:port" for a peer, looked up across every configuration map. */
    private String addressOf(String peerId) {
        String a = currentConfiguration.get(peerId);
        if (a == null) a = currentLearners.get(peerId);
        if (a == null && oldConfiguration != null) a = oldConfiguration.get(peerId);
        if (a == null) a = config.peerAddresses().get(peerId);
        return a;
    }

    /**
     * Sends the next chunk of this server's snapshot to a follower that needs
     * entries we've already compacted away (§7, Figure 13). The snapshot is
     * split into fixed-size chunks; one chunk is sent per heartbeat cycle.
     * The transfer is tracked in {@link #snapshotTransfers} and progresses
     * as each chunk is acknowledged by the follower.
     */
    private void sendInstallSnapshot(String peerId, RaftTransport transport) {
        if (witness) {
            // What a witness stores under the name of a snapshot is a compaction marker
            // with no state in it. Installed on a data node it would replace that node's
            // state with nothing. The peer needs a data node to catch up from.
            logThrottled(peerId, peerId + " is behind this witness's log (next index "
                    + nextIndex.getOrDefault(peerId, 1L) + ", log starts after "
                    + store.getSnapshotIndex() + "): a witness has no snapshot to send, "
                    + "the peer must catch up from a data node");
            return;
        }
        SnapshotTransfer transfer = snapshotTransfers.get(peerId);

        if (transfer != null && transfer.lastIncludedIndex < store.getSnapshotIndex()) {
            snapshotTransfers.remove(peerId);
            transfer = null;
        }

        if (transfer == null) {
            RaftStorage.SnapshotMeta snapshot = store.getSnapshotMeta();
            if (snapshot == null) {
                return;
            }
            if (snapshot.dataLength > Integer.MAX_VALUE) {
                logThrottled(peerId, "cannot send snapshot " + snapshot.lastIncludedIndex + " to " + peerId
                        + ": its payload of " + snapshot.dataLength + " bytes exceeds the 2 GiB the "
                        + "InstallSnapshot stream can describe");
                return;
            }
            transfer = new SnapshotTransfer(snapshot);
            snapshotTransfers.put(peerId, transfer);
        }

        if (transfer.inFlight) {
            return;
        }

        long offset = transfer.nextOffset;
        int len = (int) Math.min(config.snapshotChunkSize(), transfer.totalLength - offset);
        boolean done = (offset + len >= transfer.totalLength);
        byte[] chunk;
        try {
            // One chunk in memory at a time, read from the backend's file at the offset
            // it belongs to. The snapshot as a whole is never assembled.
            chunk = transfer.read(store, offset, len);
        } catch (IOException e) {
            logThrottled(peerId, "could not read snapshot " + transfer.lastIncludedIndex + " for "
                    + peerId + ": " + e);
            snapshotTransfers.remove(peerId);
            return;
        }
        if (chunk == null) {
            // A newer snapshot replaced the one being sent. Start over with it next round.
            snapshotTransfers.remove(peerId);
            return;
        }

        long currentTerm = store.getCurrentTerm();
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder()
                .setTerm(currentTerm)
                .setLeaderId(config.selfId())
                .setLastIncludedIndex(transfer.lastIncludedIndex)
                .setLastIncludedTerm(transfer.lastIncludedTerm)
                .setOffset(offset)
                .setData(com.google.protobuf.UnsafeByteOperations.unsafeWrap(chunk))
                .setDone(done)
                .build();

        transfer.inFlight = true;
        metrics.snapshotChunkSent();
        SnapshotTransfer transferRef = transfer;
        long newOffset = offset + len;

        transport.installSnapshot(request).whenComplete((response, t) -> {
            if (t != null) {
                lock.lock();
                try {
                    transferRef.inFlight = false;
                } finally {
                    lock.unlock();
                }
            } else {
                handleSnapshotChunkResponse(peerId, currentTerm, transferRef, newOffset, done, response);
            }
        });
    }

    private void handleSnapshotChunkResponse(String peerId, long sentTerm, SnapshotTransfer transfer,
                                              long newOffset, boolean wasFinal, InstallSnapshotResponse response) {
        lock.lock();
        try {
            transfer.inFlight = false;
            if (response.getTerm() > store.getCurrentTerm()) {
                becomeFollowerLocked(response.getTerm());
                snapshotTransfers.remove(peerId);
                return;
            }
            if (role != ServerRole.LEADER || store.getCurrentTerm() != sentTerm) {
                snapshotTransfers.remove(peerId);
                return;
            }
            if (wasFinal) {
                snapshotTransfers.remove(peerId);
                matchIndex.put(peerId, transfer.lastIncludedIndex);
                nextIndex.put(peerId, transfer.lastIncludedIndex + 1);
                advanceCommitIndex();
            } else {
                transfer.nextOffset = newOffset;
            }
        } finally {
            lock.unlock();
        }
    }

    private void handleAppendEntriesResponse(String peerId, long sentTerm, long prevLogIndex, long lastSentIndex,
                                             long sentAtMs, long sequence, AppendEntriesResponse response) {
        lock.lock();
        try {
            peerInflight.merge(peerId, -1, Integer::sum);
            if (response.getTerm() > store.getCurrentTerm()) {
                becomeFollowerLocked(response.getTerm());
                return;
            }
            if (role != ServerRole.LEADER || store.getCurrentTerm() != sentTerm) {
                return;
            }
            if (response.getSuccess()) {
                metrics.replicationSuccess();
                // v105: the last error must not outlive the failure. It was still being printed
                // in the replication status line for a peer that had long since recovered
                // (match=348 lastAck=49ms lastError=UNAVAILABLE) — a diagnostic that lies is
                // worse than none.
                peerLastFailure.remove(peerId);
                // §10.2.2: don't regress matchIndex if an older pipelined
                // response arrives after a newer one already succeeded.
                long currentMatch = matchIndex.getOrDefault(peerId, 0L);
                if (lastSentIndex > currentMatch) {
                    matchIndex.put(peerId, lastSentIndex);
                }
                advanceCommitIndex();
                peerLastAckMs.merge(peerId, sentAtMs, Math::max);
                // §4.2.1: only a voting member's acknowledgement counts toward
                // the leadership-confirmation majority behind a ReadIndex; a
                // learner's ack must never help satisfy a read barrier.
                if (isVotingMember(peerId)) {
                    // And only an answer to a request sent after the read arrived. An answer
                    // to an earlier heartbeat shows we led when the follower handled it, which
                    // was before the read; a successor may have been elected and committed a
                    // write in between, and the read would miss it (§6.4).
                    for (ReadBarrier rb : pendingReadBarriers) {
                        if (sequence > rb.sentBefore) rb.confirm(peerId);
                    }
                }
                checkReadBarriers();
                if (peerId.equals(leaderTransferTarget)) {
                    checkTransferReadyLocked();
                }
                handWitnessLeadershipOnLocked();
            } else {
                metrics.replicationFailure();
                // §10.2.2 + v118: a rejection proves the follower does NOT hold the entry at the
                // prevLogIndex we sent, so any matchIndex we recorded at/above it is a lie. Normally
                // impossible — but a wiped/rebuilt peer truncates its log back to 0 while the leader
                // still holds its pre-wipe (high) matchIndex. Clamping nextIndex to matchIndex+1 then
                // pins it above snapshotIndex forever and replicateTo never switches to InstallSnapshot
                // (the "learner stuck at 0, leader silent" tail that outlives the v117 transport fix).
                // Use the follower's reported last index to pull BOTH pointers down — monotonically,
                // since a rejection can only ever reveal the follower has less than we assumed.
                //
                // Capped at prevLogIndex, the index the follower actually refused, so nextIndex
                // always ends up below the one that just failed. The cap used to be lastSentIndex,
                // which is the same thing only for an empty heartbeat. With entries in the request
                // it lies at or above the old nextIndex, and a follower whose log is LONGER than
                // ours and differs at prevLogIndex -- a deposed leader with a tail nobody else has
                // -- reports a hint above it: nextIndex then never moved again and that follower
                // was never repaired.
                long hint = response.getConflictLastLogIndex();
                matchIndex.merge(peerId, hint, (cur, h) -> Math.min(cur, h));
                nextIndex.put(peerId, Math.max(1, Math.min(hint + 1, prevLogIndex)));
                // Try the next index now rather than at the next heartbeat: each refusal moves
                // one entry back, and at one step per heartbeat a long tail takes seconds. Handed
                // to the scheduler so that a transport which answers on the calling thread cannot
                // turn the walk back into recursion.
                if (prevLogIndex > 0) {
                    try {
                        scheduler.execute(() -> retryReplicationTo(peerId, sentTerm));
                    } catch (java.util.concurrent.RejectedExecutionException shuttingDown) {
                        // nothing left to replicate to
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    /** Replicates to {@code peerId} again, if this node still leads the term it was leading. */
    private void retryReplicationTo(String peerId, long term) {
        lock.lock();
        try {
            if (role == ServerRole.LEADER && store.getCurrentTerm() == term) {
                replicateTo(peerId);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * §10.2.1: called (possibly from the storage sync thread) when the
     * leader's own disk write has been durably fsynced. Advances the
     * leader's match index and tries to commit.
     */
    private void onLeaderDiskSyncComplete(long syncedIndex, long expectedTerm) {
        lock.lock();
        try {
            if (role != ServerRole.LEADER || store.getCurrentTerm() != expectedTerm) {
                return;
            }
            leaderDiskMatchIndex.updateAndGet(prev -> Math.max(prev, syncedIndex));
            advanceCommitIndex();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Looks for the highest index that is both replicated on a majority of
     * the *current* configuration's servers and was created in our own
     * current term, and moves commitIndex up to it. Entries from earlier
     * terms only become committed indirectly, once a later entry that
     * covers them commits -- never by counting their own replica count
     * directly (§5.4.2). The majority required is recomputed from
     * {@link #currentConfiguration} every time, since a reconfiguration can
     * change it while entries are still being committed (§6).
     */
    private void advanceCommitIndex() {
        long currentTerm = store.getCurrentTerm();
        long lastLogIndex = store.getLastLogIndex();
        for (long n = lastLogIndex; n > commitIndex.get(); n--) {
            LogEntry entry = store.getLogEntry(n);
            if (entry == null || entry.getTerm() != currentTerm) {
                continue;
            }
            if (hasSeparateMajorities(id -> matchIndex.getOrDefault(id, 0L), n)) {
                commitIndex.set(n);
                applyCommittedEntries();
                break;
            }
        }
    }

    // ------------------------------------------------------------------
    // Applying committed entries
    // ------------------------------------------------------------------

    /**
     * Feeds every newly committed entry to the state machine (or, for a
     * configuration entry, just acts on the membership change itself --
     * which already took effect when it was appended, per §6; commitment
     * only additionally triggers a leader stepping down if it just removed
     * itself) and wakes up any waiting client (§5.3, §6, §8). Finishes by
     * checking whether enough new entries have piled up since the last
     * snapshot to take another one (§7).
     */
    private void applyCommittedEntries() {
        while (lastApplied.get() < commitIndex.get()) {
            long index = lastApplied.incrementAndGet();
            LogEntry entry = store.getLogEntry(index);
            byte[] result;
            RuntimeException refusal = null;
            if (entry.hasConfiguration()) {
                result = new byte[0];
                ClusterConfiguration cfg = entry.getConfiguration();
                boolean isJoint = cfg.getOldMembersCount() > 0;
                boolean stillAMember = cfg.getMembersList().stream()
                        .anyMatch(member -> member.getId().equals(config.selfId()));
                proposeFinalConfigurationLocked(index);
                if (role == ServerRole.LEADER && !stillAMember && !isJoint) {
                    log("stepping down: committed configuration no longer includes us");
                    becomeFollowerLocked(store.getCurrentTerm());
                }
            } else if (entry.getClientId().isEmpty()) {
                result = applyToItsStateMachine(index, entry);
                if (index == leaderNoOpIndex) {
                    // A leader that inherited C_old,new from its predecessor: with its no-op
                    // committed, the joint entry before it is committed too.
                    proposeFinalConfigurationLocked(index);
                }
            } else {
                // §8: a tagged command reaches its state machine once. The entry itself
                // stays in the log either way -- it was committed, and every server has to
                // reach this same verdict from the same table at the same index. The
                // verdict comes before the routing: whether a command is a repeat does not
                // depend on which state machine it is for.
                ClientSessions.Lookup seen = clientSessions.begin(
                        entry.getClientId(), entry.getSequence(), entry.getAckedSequence());
                switch (seen.kind()) {
                    case DUPLICATE -> {
                        result = seen.result();
                        metrics.clientDuplicate();
                    }
                    case STALE -> {
                        result = null;
                        refusal = new StaleClientRequestException(entry.getClientId(), entry.getSequence());
                        metrics.clientDuplicate();
                    }
                    default -> {
                        result = applyToItsStateMachine(index, entry);
                        clientSessions.record(entry.getClientId(), entry.getSequence(), result);
                    }
                }
            }
            metrics.entryApplied();
            CompletableFuture<byte[]> pending = pendingClientRequests.remove(index);
            if (pending != null) {
                if (refusal != null) {
                    pending.completeExceptionally(refusal);
                } else {
                    pending.complete(result);
                }
            }
        }
        maybeTakeSnapshotLocked();
    }

    /**
     * Second phase of joint consensus (§6): once C_old,new is committed, the leader proposes
     * C_new. Called while applying the entry at {@code appliedIndex}; does nothing unless this
     * node leads, the configuration in effect is a joint one, and that entry has been reached.
     * Caller must hold {@code lock}.
     *
     * <p>Asked from two places, because C_old,new can commit under two leaders: the one that
     * proposed it, when the joint entry itself is applied, and a successor that found it in its
     * log, when its own no-op is applied. Without the second the cluster stays in the joint
     * configuration for good -- still correct, and dependent on a majority of servers that were
     * on their way out.
     */
    private void proposeFinalConfigurationLocked(long appliedIndex) {
        if (role != ServerRole.LEADER || oldConfiguration == null || currentConfigurationIndex > appliedIndex) {
            return;
        }
        log("joint config C_old,new committed; proposing final C_new");
        appendAndReplicateLocked(LogEntry.newBuilder()
                .setConfiguration(configProto(currentConfiguration, currentLearners)));
    }

    // ------------------------------------------------------------------
    // Log compaction / COW snapshot isolation (§7, §5.1)
    // ------------------------------------------------------------------

    /** Takes a new snapshot if enough newly applied entries have piled up since the last one. Caller must hold {@code lock}. */
    private void maybeTakeSnapshotLocked() {
        if (witness) {
            compactWitnessLogLocked(false);
            return;
        }
        if (snapshotInProgress) return;
        if (lastApplied.get() - store.getSnapshotIndex() < config.snapshotThreshold()) {
            return;
        }
        // v116: never compact while still catching up. applyCommittedEntries() runs after every
        // batch, including the partial batches a lagging follower/learner receives during catch-up
        // (commitIndex is capped at the last entry actually stored). The threshold can therefore be
        // crossed at an intermediate index while the leader has committed far beyond it -- and
        // snapshotting there would fold a mid-catch-up state and discard the log up to it before
        // the remaining entries are even applied. Only snapshot once level with what the leader
        // says is committed: the caught-up predicate from isCaughtUp() (v105) minus the lease
        // clause. A lagging lease must not stop a learner from compacting; a lagging apply must.
        // On the leader leaderCommitSeen() collapses to commitIndex, which lastApplied has just
        // been driven up to, so this is transparent there.
        if (appliedIndex() < leaderCommitSeen()) {
            return;
        }
        takeSnapshotAsync();
    }

    /**
     * Copy-on-write snapshot isolation (§5.1): the expensive work
     * (serialization + disk I/O) runs entirely outside the Raft lock.
     * Only {@link #captureCowSnapshot} runs under the lock — it must be
     * fast (shallow copy, not serialization).
     */
    private void takeSnapshotAsync() {
        long applied = lastApplied.get();
        if (applied <= store.getSnapshotIndex()) {
            return;
        }
        long includedTerm = store.getTermAt(applied);
        // Timed separately from the body below, and it is the more dangerous half: this
        // runs under the Raft lock, so whatever the state machine does here delays every
        // heartbeat and every vote. The SPI asks implementations to be cheap here, and
        // permits them not to be -- a state machine backed by a database table may read it
        // whole, because deferring a read of a mutating table is the worse mistake. Nothing said
        // how long that took.
        long captureStartNanos = System.nanoTime();
        StateMachine.SnapshotWriter cowSnapshot = captureCowSnapshot();
        metrics.captureSnapshotTimer().record(System.nanoTime() - captureStartNanos, TimeUnit.NANOSECONDS);
        byte[] configurationData = configProto(currentConfiguration, currentLearners).toByteArray();
        // Serialised here, under the lock, and not in the background task below: the
        // table has to be the one that belongs to `applied`, and the applier goes on
        // changing it the moment the lock is released. It is small -- one entry per
        // client and only the results not yet acknowledged.
        byte[] sessionData = clientSessions.toBytes();

        snapshotInProgress = true;
        snapshotExecutor.execute(() -> {
            // Timed, because this body occupies one of the scheduler's two threads, and the
            // other four users of that pool are the heartbeat, the election timer, the read
            // barrier and the transfer timeout. How long a snapshot takes is therefore a
            // statement about availability, and until this timer existed nothing measured
            // it: raft.snapshot.taken counts them, and a count cannot tell a 5 ms snapshot
            // from a 2 s one. See raft.snapshot.duration.
            long startNanos = System.nanoTime();
            try {
                if (applied <= store.getSnapshotIndex()) {
                    return;
                }
                // Written straight into the backend's file; the sink leaves the stored
                // snapshot and the log alone until commit, and drops this one there if a
                // newer snapshot was installed while it was being written.
                try (RaftStorage.SnapshotSink sink = store.beginSnapshot(applied, includedTerm)) {
                    cowSnapshot.writeTo(sink.stream());
                    sink.commit(configurationData, sessionData);
                } catch (IOException e) {
                    logWarn("snapshot through index " + applied + " failed and was discarded: " + e);
                    return;
                }
                metrics.snapshotTaken();
                metrics.takeSnapshotTimer().record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
                log("snapshotted through index " + applied
                        + " (term " + includedTerm
                        + "); log entries at or before it have been discarded");
            } finally {
                snapshotInProgress = false;
            }
        });
    }

    /**
     * COW snapshot capture (§5.1): called under the Raft lock to obtain a
     * lightweight, immutable reference to the state machine's current state.
     * The returned supplier serializes the snapshot lazily when invoked on
     * a background thread — keeping serialization and disk I/O out of the
     * critical path. Implementations like {@code KeyValueStateMachine} do a
     * fast {@code new HashMap<>(data)} here instead of serializing.
     */
    private StateMachine.SnapshotWriter captureCowSnapshot() {
        return stateMachines.prepareSnapshotWriter();
    }

    /** Synchronous snapshot for {@link #snapshotNow()} — blocks until complete. */
    private void takeSnapshotSync() {
        long applied = lastApplied.get();
        if (applied <= store.getSnapshotIndex()) {
            return;
        }
        long includedTerm = store.getTermAt(applied);
        // Timed like the async path, and this one is the worse of the two: it holds the
        // Raft lock throughout. A measurement that only covered the background path would
        // report the cheaper case.
        long startNanos = System.nanoTime();
        byte[] configurationData = configProto(currentConfiguration, currentLearners).toByteArray();
        try (RaftStorage.SnapshotSink sink = store.beginSnapshot(applied, includedTerm)) {
            stateMachines.writeSnapshot(sink.stream());
            sink.commit(configurationData, clientSessions.toBytes());
        } catch (IOException e) {
            throw new UncheckedIOException("snapshot through index " + applied + " failed", e);
        }
        metrics.snapshotTaken();
        metrics.takeSnapshotTimer().record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        log("snapshotted through index " + applied + " (term " + includedTerm
                + "); log entries at or before it have been discarded");
    }

    /**
     * Forces an immediate snapshot regardless of the configured threshold --
     * mainly useful for demos/manual testing via the {@code SNAPSHOT} CLI
     * command; normal operation triggers this on its own via
     * {@link #maybeTakeSnapshotLocked}.
     */
    public void snapshotNow() {
        lock.lock();
        try {
            if (witness) {
                compactWitnessLogLocked(true);
                return;
            }
            takeSnapshotSync();
        } finally {
            lock.unlock();
        }
    }

    /** This server's current snapshot boundary (§7), or 0 if it has never taken or installed one. */
    public long snapshotIndex() {
        return store.getSnapshotIndex();
    }

    /**
     * The highest commit index the LEADER has reported to this node (v105). A follower compares it
     * with its own {@link #appliedIndex()} to answer "am I actually up to date?" — the question
     * nobody could answer during the outage, when three nodes had an empty state machine and
     * nonetheless reported healthy.
     */
    public long leaderCommitSeen() {
        return Math.max(leaderCommitSeen, commitIndex.get());
    }

    /**
     * Is this node caught up enough to serve reads? A leader always is. A follower must have heard
     * from the leader recently AND have applied everything the leader said was committed.
     */
    public boolean isCaughtUp() {
        if (role == ServerRole.LEADER) return isReadyToServe();
        return isReadyToServe() && appliedIndex() >= leaderCommitSeen();
    }

    /**
     * The current commit index — the highest log index known to be committed on this node.
     * A direct read; for a linearizable, leader-confirmed value use {@link #readIndex()}.
     */
    public long commitIndex() {
        return commitIndex.get();
    }

    /** The current applied index — the highest log index applied to the state machine. */
    public long appliedIndex() {
        return lastApplied.get();
    }

    // ------------------------------------------------------------------
    // Client interaction (§8). A command submitted with a client id and a
    // sequence number is applied exactly once; one submitted without is
    // applied on every arrival, as it always was.
    // ------------------------------------------------------------------

    /**
     * Appends a client command to the leader's log and kicks off
     * replication. A non-leader fails fast with a hint about who the
     * leader is, mirroring how §8 describes clients being redirected.
     */
    public CompletableFuture<byte[]> submitCommand(byte[] command) {
        return submitCommand(StateMachineRegistry.APPLICATION, command);
    }

    /**
     * Like {@link #submitCommand(byte[])}, for the state machine registered under
     * {@code stateMachine}; the empty name is the application state machine. The
     * command is applied by that state machine alone, on every server.
     *
     * <p>Fails with {@link UnknownStateMachineException} and appends nothing if this
     * server has no state machine of that name.
     */
    public CompletableFuture<byte[]> submitCommand(String stateMachine, byte[] command) {
        lock.lock();
        try {
            if (witness) {
                metrics.clientRejected();
                return failedFuture(witnessRefusalLocked());
            }
            if (role != ServerRole.LEADER) {
                metrics.clientRejected();
                return failedFuture(new NotLeaderException(currentLeaderId));
            }
            if (leaderTransferTarget != null) {
                metrics.clientRejected();
                return failedFuture(new NotLeaderException(leaderTransferTarget));
            }
            if (overloadedWithWritesLocked()) {
                return failedFuture(new OverloadedException("commands", pendingClientRequests.size()));
            }
            LogEntry.Builder entry;
            try {
                entry = entryForLocked(stateMachine, command);
            } catch (RuntimeException e) {
                metrics.clientRejected();
                return failedFuture(e);
            }
            return appendAndReplicateLocked(entry);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Submits a command that takes effect exactly once however often it is submitted
     * (§8). {@code (clientId, sequence)} names the command; a caller that did not get an
     * answer submits the same pair again -- to this node or, after a leader change, to
     * the next one -- and receives the result of the first application.
     *
     * <p>Every submission is appended and committed, duplicates included: whether a
     * command is a duplicate is only known for certain at apply time, when everything
     * before it has been applied. Rejecting it earlier from the leader's current view
     * would miss the copy that is still in flight in the log.
     *
     * @param clientId      unique per client instance; must not be empty
     * @param sequence      this command's number, from 1, growing by one per new command
     * @param ackedSequence the highest sequence up to which the client has received every
     *                      response; results at or below it are dropped, and a command
     *                      at or below it fails with {@link StaleClientRequestException}
     */
    public CompletableFuture<byte[]> submitCommand(byte[] command, String clientId, long sequence, long ackedSequence) {
        return submitCommand(StateMachineRegistry.APPLICATION, command, clientId, sequence, ackedSequence);
    }

    /**
     * Like {@link #submitCommand(byte[], String, long, long)}, for the state machine
     * registered under {@code stateMachine}. A state machine that implements
     * {@link LeaderPreparation} sees every submission, a repeat included: the repeat is
     * prepared and appended again and then answered with the first result, so what its
     * {@code prepare} put into the repeat is never applied.
     */
    public CompletableFuture<byte[]> submitCommand(String stateMachine, byte[] command,
                                                   String clientId, long sequence, long ackedSequence) {
        if (clientId == null || clientId.isEmpty()) {
            return failedFuture(new IllegalArgumentException("clientId must not be empty"));
        }
        if (sequence < 1 || ackedSequence >= sequence) {
            return failedFuture(new IllegalArgumentException(
                    "sequence must be >= 1 and above ackedSequence (sequence=" + sequence
                            + ", ackedSequence=" + ackedSequence + ")"));
        }
        lock.lock();
        try {
            if (witness) {
                metrics.clientRejected();
                return failedFuture(witnessRefusalLocked());
            }
            if (role != ServerRole.LEADER) {
                metrics.clientRejected();
                return failedFuture(new NotLeaderException(currentLeaderId));
            }
            if (leaderTransferTarget != null) {
                metrics.clientRejected();
                return failedFuture(new NotLeaderException(leaderTransferTarget));
            }
            if (overloadedWithWritesLocked()) {
                return failedFuture(new OverloadedException("commands", pendingClientRequests.size()));
            }
            LogEntry.Builder entry;
            try {
                entry = entryForLocked(stateMachine, command);
            } catch (RuntimeException e) {
                metrics.clientRejected();
                return failedFuture(e);
            }
            return appendAndReplicateLocked(entry
                    .setClientId(clientId)
                    .setSequence(sequence)
                    .setAckedSequence(ackedSequence));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Builds the log entry for a client's command: chooses the state machine it is for
     * and, if that one asks for it, lets it prepare the command. Both ways of submitting
     * a command come through here, so neither can route or prepare differently.
     *
     * <p>Leader only, and before the append: the one point at which a state machine may
     * fix a non-deterministic value into the command (see {@link LeaderPreparation}).
     * What it returns is what every server applies. Caller must hold {@code lock}.
     *
     * @throws UnknownStateMachineException if nothing is registered under the name
     * @throws RuntimeException             whatever {@code prepare} threw; nothing is appended
     */
    private LogEntry.Builder entryForLocked(String stateMachine, byte[] command) {
        StateMachine target = stateMachines.require(stateMachine);
        byte[] prepared = command;
        if (target instanceof LeaderPreparation preparation) {
            prepared = preparation.prepare(command);
            if (prepared == null) {
                throw new IllegalStateException(target.getClass().getSimpleName() + ".prepare returned null");
            }
        }
        LogEntry.Builder entry = LogEntry.newBuilder();
        if (target == stateMachines.application()) {
            entry.setCommand(ByteString.copyFrom(prepared));
        } else {
            entry.setNamedCommand(NamedCommand.newBuilder()
                    .setStateMachine(stateMachine)
                    .setCommand(ByteString.copyFrom(prepared)));
        }
        return entry;
    }

    /**
     * Hands a committed command to the one state machine it is for. A name this server
     * does not have fails here on purpose: skipping a committed entry would let this
     * server's state diverge without a word.
     */
    private byte[] applyToItsStateMachine(long index, LogEntry entry) {
        if (entry.hasNamedCommand()) {
            NamedCommand namedCommand = entry.getNamedCommand();
            return stateMachines.require(namedCommand.getStateMachine())
                    .apply(index, namedCommand.getCommand().toByteArray());
        }
        // With the index: a state machine with durable storage cannot tell a first
        // application from a replay without it (see StateMachine#apply).
        return stateMachines.application().apply(index, entry.getCommand().toByteArray());
    }

    /** How many client sessions this server currently remembers. */
    public int clientSessionCount() {
        lock.lock();
        try {
            return clientSessions.size();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // Linearizable reads (ReadIndex protocol)
    // ------------------------------------------------------------------

    /**
     * Confirms that this server is still the leader and that all entries
     * up to the current commit index have been applied, so a subsequent
     * read from the state machine is linearizable. The returned future
     * completes with the leader-confirmed <em>read index</em> (the commit
     * index captured at the time of the call) once a majority of peers have
     * confirmed leadership via a heartbeat round and {@code lastApplied >=}
     * that index. Clients use this index as a linearizable read barrier.
     */
    public CompletableFuture<Long> readIndex() {
        lock.lock();
        try {
            if (witness) {
                // Covers reads asked of this node and ReadIndex asked by a follower: a
                // witness leads for the moment it takes to hand the lead on, and there
                // is nothing to gain from serving a read barrier in that moment.
                return CompletableFuture.failedFuture(witnessRefusalLocked());
            }
            if (role != ServerRole.LEADER) {
                CompletableFuture<Long> f = new CompletableFuture<>();
                f.completeExceptionally(new NotLeaderException(currentLeaderId));
                return f;
            }
            long ri = readIndexLocked();
            if (majority() == 1) {
                return lastApplied.get() >= ri
                        ? CompletableFuture.completedFuture(ri)
                        : awaitApplied(ri).thenApply(v -> ri);
            }
            int maxReads = config.maxPendingReads();
            if (maxReads > 0 && pendingReadBarriers.size() >= maxReads) {
                metrics.clientOverloaded(false);
                return CompletableFuture.failedFuture(new OverloadedException("reads", pendingReadBarriers.size()));
            }
            ReadBarrier barrier = new ReadBarrier(ri, appendEntriesSent);
            pendingReadBarriers.add(barrier);
            for (String peerId : peerTransports.keySet()) {
                replicateTo(peerId);
            }
            return barrier.future;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Serves a linearizable read (§6.4): confirms leadership with a ReadIndex
     * barrier, waits until the state machine has caught up to the index that
     * barrier captured, and only then reads.
     *
     * <p>The read itself never enters the log. What makes it linearizable is the
     * barrier: when the returned future runs, this server has proof from a
     * majority that it was still the leader when the query arrived, and every
     * command committed before then has been applied.
     *
     * <p>Fails with {@link NotLeaderException} on a follower, carrying the leader
     * hint, exactly as {@link #submitCommand} does -- a client that can find the
     * leader for writes needs no second mechanism for reads.
     */
    public CompletableFuture<byte[]> query(byte[] query) {
        return query(StateMachineRegistry.APPLICATION, query);
    }

    /**
     * Like {@link #query(byte[])}, answered by the state machine registered under
     * {@code stateMachine}; the empty name is the application state machine.
     */
    public CompletableFuture<byte[]> query(String stateMachine, byte[] query) {
        StateMachine target;
        try {
            target = stateMachines.require(stateMachine);
        } catch (UnknownStateMachineException e) {
            return failedFuture(e);
        }
        return readIndex().thenApply(readIndex -> target.read(query));
    }

    /**
     * Serves a read through the leader lease (§6.4's second option) rather than a
     * ReadIndex barrier: no heartbeat round-trip, the answer comes straight out of
     * the state machine.
     *
     * <p>What this buys and what it costs is on {@link #leaseRead()}. What is worth
     * saying <em>here</em>, because it is the thing people expect and do not get:
     * this is not a switch that serves stale data. {@link #leaseRead()} falls back
     * to {@link #readIndex()} whenever the lease is not currently held, and the
     * lease is held only while a quorum has acknowledged a heartbeat inside
     * {@code ELECTION_TIMEOUT_MIN_MS}. A leader cut off from its majority loses the
     * lease within that window and this method then blocks exactly as
     * {@link #query} would.
     *
     * <p>So the exposure is not partition, it is <em>time</em>: the window between
     * the last quorum acknowledgement and the moment the lease is reconsidered. A
     * leader whose clock jumps, or whose process is paused past that window and
     * resumes believing no time passed, can answer from a lease it no longer holds.
     * That is the assumption slide 22 asks you to accept knowingly -- it is about
     * clocks, not about cables.
     */
    public CompletableFuture<byte[]> queryWithLease(byte[] query) {
        return queryWithLease(StateMachineRegistry.APPLICATION, query);
    }

    /** Like {@link #queryWithLease(byte[])}, answered by the named state machine. */
    public CompletableFuture<byte[]> queryWithLease(String stateMachine, byte[] query) {
        StateMachine target;
        try {
            target = stateMachines.require(stateMachine);
        } catch (UnknownStateMachineException e) {
            return failedFuture(e);
        }
        return leaseRead().thenApply(ignored -> target.read(query));
    }

    /** The names of the state machines registered beside the application's. */
    public java.util.Set<String> stateMachineNames() {
        return stateMachines.names();
    }

    /**
     * Lease-based linearizable read: serves the read immediately (no
     * heartbeat round-trip) if a majority of peers have acknowledged
     * a heartbeat within the election timeout window, proving no other
     * leader could have been elected. Falls back to {@link #readIndex()}
     * if the lease has expired.
     *
     * <p>Assumes bounded clock skew across the cluster. If clocks can
     * diverge by more than {@code ELECTION_TIMEOUT_MIN_MS}, use
     * {@link #readIndex()} instead for safety.
     */
    public CompletableFuture<Void> leaseRead() {
        lock.lock();
        try {
            if (witness) {
                return CompletableFuture.failedFuture(witnessRefusalLocked());
            }
            if (role != ServerRole.LEADER) {
                CompletableFuture<Void> f = new CompletableFuture<>();
                f.completeExceptionally(new NotLeaderException(currentLeaderId));
                return f;
            }
            if (majority() == 1 || hasValidLease()) {
                long ri = readIndexLocked();
                return lastApplied.get() >= ri
                        ? CompletableFuture.completedFuture(null)
                        : awaitApplied(ri);
            }
            return readIndex().thenApply(ignored -> null);
        } finally {
            lock.unlock();
        }
    }

    /**
     * The index a read arriving now must see. Caller must hold {@code lock} and be leader.
     *
     * <p>Not simply {@code commitIndex}. A leader that has just been elected holds every
     * committed entry (Leader Completeness) but does not yet know which of them are
     * committed: its commit index is whatever it had heard as a follower, and can lie
     * behind a write its predecessor already acknowledged to a client. §6.4 step 1
     * therefore requires an entry of the leader's own term to commit first. Taking the
     * no-op's index as the floor does that without a special case: the read cannot be
     * served until the no-op, and so everything before it, has been applied.
     */
    private long readIndexLocked() {
        return Math.max(commitIndex.get(), leaderNoOpIndex);
    }

    // ------------------------------------------------------------------
    // Follower reads (§6.4): the leader names the index, the follower reads
    // ------------------------------------------------------------------

    /** How long a follower waits to apply up to the read index before it gives the read up. */
    private static final long FOLLOWER_READ_APPLY_TIMEOUT_MS = 2_000;

    /**
     * Leader side of a follower read: confirms leadership exactly as for a read of its
     * own and hands the requester the index. The requester must be a member or learner
     * of the current configuration; a heartbeat round is not owed to anyone else.
     */
    @Override
    public CompletableFuture<ReadIndexResponse> handleReadIndex(ReadIndexRequest request) {
        String requester = request.getRequesterId();
        long term;
        lock.lock();
        try {
            term = store.getCurrentTerm();
            if (role != ServerRole.LEADER) {
                ReadIndexResponse.Builder refusal = ReadIndexResponse.newBuilder().setTerm(term);
                if (currentLeaderId != null) refusal.setLeaderHint(currentLeaderId);
                return CompletableFuture.completedFuture(refusal.build());
            }
            if (!votingMembers().contains(requester) && !currentLearners.containsKey(requester)) {
                logWarn("refusing ReadIndex from " + requester + ": not a member or learner");
                return CompletableFuture.completedFuture(ReadIndexResponse.newBuilder().setTerm(term).build());
            }
        } finally {
            lock.unlock();
        }
        metrics.readIndexServed();
        return readIndex().handle((index, failure) -> {
            ReadIndexResponse.Builder response = ReadIndexResponse.newBuilder().setTerm(term);
            if (failure == null) {
                return response.setSuccess(true).setReadIndex(index).build();
            }
            // Lost the leadership while confirming it: that is the case the round exists
            // to catch, and the answer is "not me", with whoever it is now if known.
            String hint = currentLeaderId;
            if (hint != null && !hint.equals(config.selfId())) response.setLeaderHint(hint);
            return response.build();
        });
    }

    /**
     * A read index this server may read at, whatever its role (§6.4).
     *
     * <p>On the leader this is {@link #readIndex()}. On a follower or learner it asks the
     * leader for the index with the ReadIndex RPC and completes once this server has
     * applied that far -- so a state machine read after it sees every command committed
     * before the call was made, which is linearizability, on a server that is not the
     * leader.
     *
     * <p>Fails with {@link NotLeaderException} if no leader is known or the server asked
     * turned out not to be it, and with {@link FollowerReadException} if this server does
     * not catch up to the index in time. A partitioned follower therefore fails the read;
     * it cannot serve a stale one, because it cannot obtain an index.
     */
    public CompletableFuture<Long> readIndexAnyRole() {
        String leaderId;
        RaftTransport toLeader;
        lock.lock();
        try {
            if (witness) {
                return CompletableFuture.failedFuture(witnessRefusalLocked());
            }
            if (role == ServerRole.LEADER) {
                return readIndex();
            }
            leaderId = currentLeaderId;
            toLeader = leaderId == null ? null : peerTransports.get(leaderId);
        } finally {
            lock.unlock();
        }
        if (toLeader == null) {
            return CompletableFuture.failedFuture(new NotLeaderException(leaderId));
        }
        // Sent without the lock: the answer arrives on a transport thread, and the leader
        // will be sending this server a heartbeat in the meantime that needs the lock.
        return toLeader.readIndex(ReadIndexRequest.newBuilder().setRequesterId(config.selfId()).build())
                .thenCompose(response -> {
                    if (!response.getSuccess()) {
                        String hint = response.getLeaderHint().isEmpty() ? null : response.getLeaderHint();
                        return CompletableFuture.<Long>failedFuture(new NotLeaderException(hint));
                    }
                    long index = response.getReadIndex();
                    long deadline = System.nanoTime()
                            + TimeUnit.MILLISECONDS.toNanos(FOLLOWER_READ_APPLY_TIMEOUT_MS);
                    return awaitAppliedBy(index, deadline).thenApply(ignored -> index);
                });
    }

    /**
     * A linearizable read answered by this server whatever its role: the leader names the
     * index ({@link #readIndexAnyRole()}), this server's own state machine answers.
     */
    public CompletableFuture<byte[]> queryAnyRole(byte[] query) {
        return queryAnyRole(StateMachineRegistry.APPLICATION, query);
    }

    /** Like {@link #queryAnyRole(byte[])}, answered by the named state machine. */
    public CompletableFuture<byte[]> queryAnyRole(String stateMachine, byte[] query) {
        StateMachine target;
        try {
            target = stateMachines.require(stateMachine);
        } catch (UnknownStateMachineException e) {
            return failedFuture(e);
        }
        boolean follower = role != ServerRole.LEADER;
        return readIndexAnyRole().thenApply(index -> {
            if (follower) metrics.followerReadServed();
            return target.read(query);
        });
    }

    /** As {@link #awaitApplied}, but gives up at {@code deadlineNanos} instead of polling forever. */
    private CompletableFuture<Void> awaitAppliedBy(long targetIndex, long deadlineNanos) {
        CompletableFuture<Void> done = new CompletableFuture<>();
        pollApplied(targetIndex, deadlineNanos, done);
        return done;
    }

    /**
     * Whether the state machine holds the effect of every entry up to {@code targetIndex}.
     *
     * <p>Under the lock, and that is the whole point. {@code applyCommittedEntries} advances
     * {@code lastApplied} first and calls the state machine second, so the counter alone is
     * ahead of the state for the duration of one {@code apply}. A read that trusted the
     * bare counter could be let through in that gap and answer from before the very entry
     * it had waited for -- seen as a follower read returning the previous value once in a
     * few hundred, over the slowest transport. Applying happens under the lock, so holding
     * it here means no apply is half done.
     */
    private boolean hasApplied(long targetIndex) {
        lock.lock();
        try {
            return lastApplied.get() >= targetIndex;
        } finally {
            lock.unlock();
        }
    }

    private void pollApplied(long targetIndex, long deadlineNanos, CompletableFuture<Void> done) {
        if (hasApplied(targetIndex)) {
            done.complete(null);
        } else if (System.nanoTime() >= deadlineNanos) {
            done.completeExceptionally(new FollowerReadException("this server has applied "
                    + lastApplied.get() + " and the read needs " + targetIndex + "; it is too far behind"));
        } else {
            try {
                scheduler.schedule(() -> pollApplied(targetIndex, deadlineNanos, done), 2, TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.RejectedExecutionException shuttingDown) {
                done.completeExceptionally(new FollowerReadException("this server is shutting down"));
            }
        }
    }

    /**
     * Do we still hold a lease — i.e. has a quorum acknowledged us within the election-timeout
     * window?
     *
     * <p>This used to count acknowledgements only within {@code currentConfiguration} and compare
     * them against a single {@code majority()}. During a configuration change that ignores
     * C_old entirely: a leader that had lost contact with a majority of the servers it is being
     * replaced by (or replacing) still reported a healthy lease — and therefore
     * {@code isReadyToServe()}, a {@code leaseRead()}, and an UP health check.
     */
    private boolean hasValidLease() {
        long now = nowMs();
        long window = ELECTION_TIMEOUT_MIN_MS;

        java.util.Set<String> fresh = new java.util.HashSet<>();
        fresh.add(config.selfId());
        for (String peerId : votingMembers()) {
            if (peerId.equals(config.selfId())) continue;
            Long lastAck = peerLastAckMs.get(peerId);
            if (lastAck != null && now - lastAck < window) {
                fresh.add(peerId);
            }
        }
        return Quorum.reached(fresh, currentConfiguration, oldConfiguration);
    }

    /** Every voting member, across both configurations while a change is in flight. */
    private java.util.Set<String> votingMembers() {
        java.util.Set<String> ids = new java.util.HashSet<>(currentConfiguration.keySet());
        Map<String, String> old = oldConfiguration;
        if (old != null) ids.addAll(old.keySet());
        return ids;
    }

    private CompletableFuture<Void> awaitApplied(long targetIndex) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        scheduler.schedule(() -> {
            if (hasApplied(targetIndex)) {
                f.complete(null);
            } else {
                awaitApplied(targetIndex).whenComplete((v, t) -> {
                    if (t != null) f.completeExceptionally(t); else f.complete(null);
                });
            }
        }, 5, TimeUnit.MILLISECONDS);
        return f;
    }

    private void checkReadBarriers() {
        pendingReadBarriers.removeIf(barrier -> {
            if (confirmsLeadership(barrier) && lastApplied.get() >= barrier.readIndex) {
                barrier.future.complete(barrier.readIndex);
                return true;
            }
            return false;
        });
    }

    /**
     * Has a quorum confirmed that we are still the leader (§6.4)? Correct across a configuration
     * change: a majority of C_old AND of C_new, not a count over the union — see {@link Quorum}.
     */
    private boolean confirmsLeadership(ReadBarrier barrier) {
        java.util.Set<String> acks = new java.util.HashSet<>(barrier.confirmed);
        acks.add(config.selfId());   // the leader's own confirmation
        return Quorum.reached(acks, currentConfiguration, oldConfiguration);
    }

    /**
     * A pending ReadIndex (§6.4): the index a read must see, and who has confirmed our leadership
     * so far.
     *
     * <p>It used to carry a single {@code needed} count and answer {@code confirmed.size() + 1 >=
     * needed}. That is wrong during a configuration change: joint consensus requires a majority in
     * C<sub>old</sub> AND in C<sub>new</sub>, which no count over the union can express (see
     * {@link Quorum}). The barrier now just records WHO confirmed, and the node decides whether
     * that is a quorum — using the same predicate as every other quorum decision.
     */
    private static final class ReadBarrier {
        final long readIndex;
        /** How many AppendEntries had been sent when the read arrived; their answers do not count. */
        final long sentBefore;
        final CompletableFuture<Long> future = new CompletableFuture<>();
        final java.util.Set<String> confirmed = ConcurrentHashMap.newKeySet();

        ReadBarrier(long readIndex, long sentBefore) {
            this.readIndex = readIndex;
            this.sentBefore = sentBefore;
        }

        void confirm(String peerId) { confirmed.add(peerId); }
    }

    // ------------------------------------------------------------------
    // Leadership transfer (§3.10)
    // ------------------------------------------------------------------

    /**
     * Transfers leadership to {@code targetId}. The leader stops accepting
     * new client requests, ensures the target's log is fully caught up,
     * then sends a {@code TimeoutNow} RPC so the target starts an election
     * immediately. If the transfer doesn't complete within an election
     * timeout, the leader aborts and resumes normal operation.
     */
    public CompletableFuture<Void> transferLeadership(String targetId) {
        lock.lock();
        try {
            if (witness) {
                // A witness hands the lead on by itself, to whoever can take it first.
                return CompletableFuture.failedFuture(witnessRefusalLocked());
            }
            return startLeadershipTransferLocked(targetId);
        } finally {
            lock.unlock();
        }
    }

    private CompletableFuture<Void> startLeadershipTransferLocked(String targetId) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        lock.lock();
        try {
            if (role != ServerRole.LEADER) {
                result.completeExceptionally(new NotLeaderException(currentLeaderId));
                return result;
            }
            if (!currentConfiguration.containsKey(targetId)) {
                result.completeExceptionally(new IllegalArgumentException(
                        targetId + " is not a current cluster member"));
                return result;
            }
            if (targetId.equals(config.selfId())) {
                result.complete(null);
                return result;
            }
            // A second transfer while one is in flight used to overwrite the target and
            // the future of the first: the pending timeout then completed the NEW request
            // exceptionally and the old caller waited forever. Unreachable while the only
            // way in was a human at a JMX console; a client that retries -- which is what
            // TransferLeadership over RaftClientService made possible -- walks straight
            // into it. Refuse, in the words the caller is meant to act on.
            if (leaderTransferTarget != null) {
                result.completeExceptionally(new IllegalStateException(
                        "a leadership transfer to " + leaderTransferTarget
                                + " is already in flight; retry once it has finished"));
                return result;
            }

            leaderTransferTarget = targetId;
            leaderTransferResult = result;
            leaderTransferSent = false;
            log("starting leadership transfer to " + targetId);

            leaderTransferTimeout = scheduler.schedule(() -> {
                lock.lock();
                try {
                    if (leaderTransferTarget != null) {
                        log("leadership transfer to " + leaderTransferTarget + " timed out, aborting");
                        clearTransferLocked();
                        // Ends in "retry" on purpose: that word is how this class marks
                        // every rejection a caller is meant to repeat, and a transfer that
                        // ran out of time is one -- the leader has aborted and resumed
                        // normal service, so asking again is both safe and usually enough.
                        // The target was most likely still catching up.
                        result.completeExceptionally(new RuntimeException(
                                "leadership transfer timed out; retry once the target has caught up"));
                    }
                } finally {
                    lock.unlock();
                }
            }, ELECTION_TIMEOUT_MAX_MS, TimeUnit.MILLISECONDS);

            replicateTo(targetId);
            checkTransferReadyLocked();
        } finally {
            lock.unlock();
        }
        return result;
    }

    /** Forget the transfer in flight: writes are accepted again and the abort timer is off. */
    private void clearTransferLocked() {
        leaderTransferTarget = null;
        leaderTransferResult = null;
        leaderTransferSent = false;
        if (leaderTransferTimeout != null) {
            leaderTransferTimeout.cancel(false);
            leaderTransferTimeout = null;
        }
    }

    private void checkTransferReadyLocked() {
        String targetId = leaderTransferTarget;
        CompletableFuture<Void> result = leaderTransferResult;
        if (targetId == null || result == null) return;
        // One TimeoutNow per transfer. See leaderTransferSent: without this, every
        // AppendEntries response that arrives before the first call's callback sends another,
        // and each one restarts the target's election instead of letting it finish.
        if (leaderTransferSent) return;

        long targetMatch = matchIndex.getOrDefault(targetId, 0L);
        if (targetMatch < store.getLastLogIndex()) return;

        RaftTransport transport = peerTransports.get(targetId);
        if (transport == null) return;

        leaderTransferSent = true;
        log("target " + targetId + " is caught up, sending TimeoutNow");
        long currentTerm = store.getCurrentTerm();
        transport.timeoutNow(TimeoutNowRequest.newBuilder()
                .setTerm(currentTerm).setLeaderId(config.selfId()).build()).whenComplete((resp, t) -> {
            lock.lock();
            try {
                // The block on new commands stays up until the transfer is decided, not until
                // this RPC is answered. TimeoutNow is acknowledged in about a millisecond; the
                // target's election takes longer, and this node remains leader meanwhile. Every
                // command accepted in that window appends an entry the candidate does not have,
                // which makes its log stale -- and §5.4.1 then tells every voter to refuse it.
                //
                // Measured on the Pi cluster on 2026-11-25 under 200 writes/s: TimeoutNow out
                // at 22:00:05.832, this node still leading until 22:00:05.844, and in those
                // twelve milliseconds the target's campaign became unwinnable. It got no vote
                // from anyone, the cluster went 0.30 s without a commit, 92 writes were refused,
                // and the leadership ended up on a third node -- the one demo that needed the
                // target to lead was skipped as a result. Transfers "worked" whenever the gap
                // happened to be empty, which is why an idle cluster never showed this.
                //
                // So: on the way out, only the failure case clears the state. Success leaves it
                // standing for becomeFollowerLocked (the transfer worked) or for the abort
                // timer (it did not), both of which already clear it.
                //
                // A failed TimeoutNow response is not a failed transfer, and the case where
                // it lies is the case where everything went right. The target campaigns the
                // instant it receives the message, wins, and this node steps down -- which
                // tears down the very call whose answer we are waiting for. gRPC reports
                // CANCELLED. Measured in a rehearsal against the Pi cluster: the operator
                // command printed "the cluster refused (CANCELLED: io.grpc.Context was
                // cancelled without error)" while the run's own leader log recorded
                // "node3 -> node1 (gap 0.00 s)". The faster the transfer works, the more
                // reliably it used to report failure.
                //
                // So the outcome is read from the thing that is actually observable here:
                // whether this node is still the leader. If it is not, the transfer did what
                // it was asked to do, whatever became of the response. If it still is,
                // nothing happened and the error stands.
                if (t == null && resp != null && resp.getWitness()) {
                    // The one answer that settles it at once: the target will not campaign.
                    knownWitnesses.add(targetId);
                    if (targetId.equals(leaderTransferTarget)) {
                        clearTransferLocked();
                    }
                    result.completeExceptionally(new IllegalStateException(
                            targetId + " is a witness and cannot lead; choose a data node"));
                } else if (t == null && resp != null && resp.getDeclined()) {
                    // The target will not campaign: it does not follow this node. Without this
                    // the answer looked like any other and the caller was told the target had
                    // been asked to stand, while this node went on leading.
                    if (targetId.equals(leaderTransferTarget)) {
                        clearTransferLocked();
                    }
                    result.completeExceptionally(new IllegalStateException(
                            targetId + " declined the transfer: it does not follow this node as its "
                                    + "leader yet; retry"));
                } else if (t != null && role == ServerRole.LEADER) {
                    // Nothing happened: release the block and let writes through again.
                    clearTransferLocked();
                    result.completeExceptionally(t);
                } else {
                    result.complete(null);
                }
            } finally {
                lock.unlock();
            }
        });
    }

    @Override
    public TimeoutNowResponse handleTimeoutNow(TimeoutNowRequest request) {
        lock.lock();
        try {
            // Only the leader this node follows may hand it the lead. The transport has already
            // established, under mTLS, that the sender is who it says it is; this is the other
            // half -- that who it is, is the leader. Without it a truthful non-leader (a learner
            // with a perfectly valid certificate) could still make this node campaign.
            //
            // An empty id is a peer from before the field existed. It is let through here so a
            // plaintext cluster can be upgraded node by node; on an mTLS cluster it never gets
            // this far, because an empty claim does not match any certificate.
            String claimed = request.getLeaderId();
            if (!claimed.isEmpty() && !claimed.equals(currentLeaderId)) {
                log("ignoring TimeoutNow from " + claimed + ": it is not the leader this node "
                        + "follows (" + currentLeaderId + ")");
                return TimeoutNowResponse.newBuilder().setDeclined(true).build();
            }
            if (witness) {
                log("declining TimeoutNow" + (claimed.isEmpty() ? "" : " from " + claimed)
                        + ": this node is a witness and does not take the lead on request");
                return TimeoutNowResponse.newBuilder().setWitness(true).build();
            }
            log("received TimeoutNow" + (claimed.isEmpty() ? "" : " from " + claimed)
                    + ", starting immediate election");
            // The incumbent asked us to take over, so our RequestVotes must carry the transfer
            // flag -- every other voter still hears from that (very much alive) leader and would
            // otherwise refuse us on stickiness grounds.
            startRealElection(store.getCurrentTerm() + 1, true);
            return TimeoutNowResponse.getDefaultInstance();
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // Cluster reconfiguration (§6). Two mechanisms coexist here:
    //
    //  * Single-server changes (addServer / removeServer / addLearner /
    //    promoteLearner / removeLearner): changing only one member per step
    //    guarantees the old and new configurations' majorities always share
    //    at least one server, so such a change is just one more log entry,
    //    replicated and committed like a normal command — no joint phase
    //    needed. A second change has to wait for the first to commit
    //    (enforced in rejectIfConfigurationChangePending).
    //
    //  * Arbitrary changes (setConfiguration): full joint consensus
    //    C_old,new. While the joint entry is in effect, every majority
    //    decision (votes, commits, ReadIndex, lease — see Quorum) requires
    //    separate majorities in BOTH configurations; once C_old,new commits,
    //    the leader automatically proposes the final C_new (see
    //    applyCommittedEntries).
    // ------------------------------------------------------------------

    /** Adds a new voting member. Only the leader can do this (§6). */
    public CompletableFuture<byte[]> addServer(String id, String address) {
        lock.lock();
        try {
            if (role != ServerRole.LEADER) {
                return failedFuture(new NotLeaderException(currentLeaderId));
            }
            if (currentConfiguration.containsKey(id)) {
                return failedFuture(new ConfigurationChangeException(id + " is already a member"));
            }
            CompletableFuture<byte[]> rejected = rejectIfConfigurationChangePending();
            if (rejected != null) {
                return rejected;
            }

            Map<String, String> updated = new LinkedHashMap<>(currentConfiguration);
            updated.put(id, address);
            // If the id is currently a learner, adding it as a voter also
            // removes it from the learner set (that is exactly what
            // promoteLearner does; addServer of a known learner is treated the
            // same, since a member is never simultaneously a learner).
            Map<String, String> updatedLearners = withoutKey(currentLearners, id);
            log("proposing to add " + id + " (" + address + ")");
            return appendAndReplicateLocked(LogEntry.newBuilder().setConfiguration(configProto(updated, updatedLearners)));
        } finally {
            lock.unlock();
        }
    }

    /** Removes an existing voting member. Only the leader can do this (§6). */
    public CompletableFuture<byte[]> removeServer(String id) {
        lock.lock();
        try {
            if (role != ServerRole.LEADER) {
                return failedFuture(new NotLeaderException(currentLeaderId));
            }
            if (!currentConfiguration.containsKey(id)) {
                return failedFuture(new ConfigurationChangeException(id + " is not a current member"));
            }
            if (currentConfiguration.size() <= 1) {
                return failedFuture(new ConfigurationChangeException("refusing to remove the last member of the cluster"));
            }
            CompletableFuture<byte[]> rejected = rejectIfConfigurationChangePending();
            if (rejected != null) {
                return rejected;
            }

            Map<String, String> updated = new LinkedHashMap<>(currentConfiguration);
            updated.remove(id);
            log("proposing to remove " + id);
            return appendAndReplicateLocked(LogEntry.newBuilder().setConfiguration(configProto(updated, currentLearners)));
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // Non-voting learners (§4.2.1 of Ongaro's dissertation)
    // ------------------------------------------------------------------

    /**
     * Adds a non-voting learner: the leader begins replicating the log to
     * {@code id} immediately, but it takes no part in elections or commit
     * majorities. This is the safe way to introduce a server that starts far
     * behind -- it catches up as a learner and is only later promoted to a
     * voting member with {@link #promoteLearner}, so the voting majority is
     * never enlarged to include a server that has yet to receive the log.
     * Only the leader can do this.
     */
    public CompletableFuture<byte[]> addLearner(String id, String address) {
        lock.lock();
        try {
            if (role != ServerRole.LEADER) {
                return failedFuture(new NotLeaderException(currentLeaderId));
            }
            if (currentConfiguration.containsKey(id)) {
                return failedFuture(new ConfigurationChangeException(id + " is already a voting member"));
            }
            if (currentLearners.containsKey(id)) {
                return failedFuture(new ConfigurationChangeException(id + " is already a learner"));
            }
            CompletableFuture<byte[]> rejected = rejectIfConfigurationChangePending();
            if (rejected != null) {
                return rejected;
            }

            Map<String, String> updatedLearners = new LinkedHashMap<>(currentLearners);
            updatedLearners.put(id, address);
            log("proposing to add learner " + id + " (" + address + ")");
            return appendAndReplicateLocked(
                    LogEntry.newBuilder().setConfiguration(configProto(currentConfiguration, updatedLearners)));
        } finally {
            lock.unlock();
        }
    }

    /** Removes a non-voting learner (e.g. one that will not be promoted). Only the leader can do this. */
    public CompletableFuture<byte[]> removeLearner(String id) {
        lock.lock();
        try {
            if (role != ServerRole.LEADER) {
                return failedFuture(new NotLeaderException(currentLeaderId));
            }
            if (!currentLearners.containsKey(id)) {
                return failedFuture(new ConfigurationChangeException(id + " is not a current learner"));
            }
            CompletableFuture<byte[]> rejected = rejectIfConfigurationChangePending();
            if (rejected != null) {
                return rejected;
            }

            Map<String, String> updatedLearners = new LinkedHashMap<>(currentLearners);
            updatedLearners.remove(id);
            log("proposing to remove learner " + id);
            return appendAndReplicateLocked(
                    LogEntry.newBuilder().setConfiguration(configProto(currentConfiguration, updatedLearners)));
        } finally {
            lock.unlock();
        }
    }

    /**
     * Promotes a caught-up learner to a voting member (§4.2.1): a single-server
     * membership change moving {@code id} from the learner set into the voting
     * configuration. Rejected unless the learner's {@code matchIndex} has
     * reached the leader's commit index, so the enlarged majority never
     * includes a server that is still missing committed entries (which could
     * otherwise stall commits until it catches up). Only the leader can do
     * this.
     */
    public CompletableFuture<byte[]> promoteLearner(String id) {
        lock.lock();
        try {
            if (role != ServerRole.LEADER) {
                return failedFuture(new NotLeaderException(currentLeaderId));
            }
            if (currentConfiguration.containsKey(id)) {
                return failedFuture(new ConfigurationChangeException(id + " is already a voting member"));
            }
            if (!currentLearners.containsKey(id)) {
                return failedFuture(new ConfigurationChangeException(id + " is not a learner"));
            }
            CompletableFuture<byte[]> rejected = rejectIfConfigurationChangePending();
            if (rejected != null) {
                return rejected;
            }
            long match = matchIndex.getOrDefault(id, 0L);
            long target = commitIndex.get();
            if (match < target) {
                return failedFuture(new ConfigurationChangeException(
                        "learner " + id + " has not caught up yet (matchIndex " + match
                                + " < commitIndex " + target + "); retry once it has"));
            }

            String address = currentLearners.get(id);
            Map<String, String> updatedMembers = new LinkedHashMap<>(currentConfiguration);
            updatedMembers.put(id, address);
            Map<String, String> updatedLearners = withoutKey(currentLearners, id);
            log("promoting learner " + id + " to voting member");
            return appendAndReplicateLocked(
                    LogEntry.newBuilder().setConfiguration(configProto(updatedMembers, updatedLearners)));
        } finally {
            lock.unlock();
        }
    }

    private static Map<String, String> withoutKey(Map<String, String> map, String key) {
        if (!map.containsKey(key)) return map;
        Map<String, String> copy = new LinkedHashMap<>(map);
        copy.remove(key);
        return copy;
    }

    /**
     * Joint consensus (§6): atomically replaces the entire cluster membership.
     * This two-phase approach is safe for arbitrary changes (adding and removing
     * multiple servers at once). Phase 1 appends C_old,new (requiring majorities
     * from both old and new configs); phase 2 is triggered automatically when
     * C_old,new commits, appending C_new (the final configuration).
     */
    public CompletableFuture<byte[]> setConfiguration(Map<String, String> newMembers) {
        lock.lock();
        try {
            if (role != ServerRole.LEADER) {
                return failedFuture(new NotLeaderException(currentLeaderId));
            }
            if (newMembers.isEmpty()) {
                return failedFuture(new ConfigurationChangeException("new configuration must not be empty"));
            }
            CompletableFuture<byte[]> rejected = rejectIfConfigurationChangePending();
            if (rejected != null) {
                return rejected;
            }
            if (newMembers.equals(currentConfiguration)) {
                return CompletableFuture.completedFuture(new byte[0]);
            }

            log("proposing joint configuration C_old,new -> " + newMembers.keySet());
            ClusterConfiguration joint = toJointProto(currentConfiguration, newMembers, currentLearners);
            return appendAndReplicateLocked(LogEntry.newBuilder().setConfiguration(joint));
        } finally {
            lock.unlock();
        }
    }

    /** Returns a failed future if a config change isn't safe right now, or null if it's fine to proceed. */
    private CompletableFuture<byte[]> rejectIfConfigurationChangePending() {
        if (witness) {
            // Reached only while a witness leads. It is on its way out; the change
            // belongs to the data node it hands the lead to.
            return failedFuture(witnessRefusalLocked());
        }
        // §4 errata: a leader must commit an entry from its own term
        // before accepting config changes, so it knows the latest
        // committed configuration. The no-op serves this purpose.
        if (commitIndex.get() < leaderNoOpIndex) {
            return failedFuture(new ConfigurationChangeException(
                    "leader has not yet committed an entry in its current term; retry shortly"));
        }
        if (currentConfigurationIndex > commitIndex.get()) {
            return failedFuture(new ConfigurationChangeException(
                    "a previous configuration change has not committed yet; retry once it has"));
        }
        return null;
    }

    /** Appends entryBuilder as the next log entry and replicates it to every current peer. Caller must hold {@code lock}. */
    private CompletableFuture<byte[]> appendAndReplicateLocked(LogEntry.Builder entryBuilder) {
        long index = store.getLastLogIndex() + 1;
        long appendTerm = store.getCurrentTerm();
        LogEntry entry = entryBuilder.setIndex(index).setTerm(appendTerm).build();

        CompletableFuture<byte[]> future = new CompletableFuture<>();
        pendingClientRequests.put(index, future);

        // §10.2.1: write the entry so it's readable for replication but
        // defer the fsync — replication starts in parallel with the disk
        // sync, and leaderDiskMatchIndex advances when the sync completes.
        store.appendEntriesDeferSync(List.of(entry)).thenRun(() ->
                onLeaderDiskSyncComplete(index, appendTerm));

        if (entry.hasConfiguration()) {
            // §6: the latest configuration in our log governs immediately,
            // even before it commits -- including which peers we replicate
            // this very entry to below.
            applyConfigurationLocked(entry.getIndex(), entry.getConfiguration());
        }

        for (String peerId : peerTransports.keySet()) {
            replicateTo(peerId);
        }
        advanceCommitIndex(); // handles the single-node-cluster case immediately
        return future;
    }

    private static CompletableFuture<byte[]> failedFuture(RuntimeException e) {
        CompletableFuture<byte[]> failed = new CompletableFuture<>();
        failed.completeExceptionally(e);
        return failed;
    }

    public RaftMetrics metrics() { return metrics; }

    public ServerRole role() { return role; }

    /** Current election epoch -- see {@link #electionEpoch}. Package-private, for tests. */
    long electionEpoch() { return electionEpoch.get(); }

    /**
     * The timeout, in milliseconds, that the currently armed election timer was scheduled with;
     * 0 before the first one is armed. Package-private, for tests: the boot delay
     * ({@link #BOOT_DELAY_FACTOR}) is otherwise only observable by waiting out a real timer, and
     * a test that sleeps for a second to assert a delay is a test that will one day fail on a
     * loaded build machine for reasons unrelated to the delay.
     */
    volatile int lastArmedElectionTimeoutMs = 0;

    public String currentLeaderId() { return currentLeaderId; }

    public boolean isTransferInProgress() { return leaderTransferTarget != null; }

    /**
     * Health/readiness signal: can this node currently make progress on client work?
     *
     * <p>Quorum-aware, unlike a bare {@code role}/{@code currentLeaderId} check:
     * <ul>
     *   <li>a {@code LEADER} is ready only while it holds a fresh lease — a majority of the
     *       configuration (counting itself) acknowledged within the election-timeout window
     *       ({@link #hasValidLease()}); a leader that has lost quorum is <em>not</em> ready even
     *       though it still reports {@code role=LEADER};</li>
     *   <li>a follower/candidate is ready only if it has heard from a valid leader within the
     *       election-timeout window ({@link #hasLeaderStickiness()}); a node that still remembers a
     *       now-unreachable leader ({@code currentLeaderId != null}) is <em>not</em> ready;</li>
     *   <li>a single-node cluster ({@code majority() == 1}) is always ready while leader.</li>
     * </ul>
     *
     * <p>Reads volatile/concurrent state without taking the node lock, so a health probe never
     * contends with consensus work; the result may be momentarily stale, which is acceptable for
     * a readiness signal.
     */
    public boolean isReadyToServe() {
        if (role == ServerRole.LEADER) {
            return majority() == 1 || hasValidLease();
        }
        return hasLeaderStickiness();
    }

    /** This server's current view of the cluster's <em>voting</em> membership ("id" -> "host:port"), including itself if it is a voter. */
    public Map<String, String> currentConfiguration() {
        return currentConfiguration;
    }

    /** This server's current view of the non-voting learner set ("id" -> "host:port"), §4.2.1. */
    public Map<String, String> currentLearners() {
        return currentLearners;
    }

    /** True if this node is itself currently a non-voting learner (§4.2.1). */
    public boolean isLearner() {
        return currentLearners.containsKey(config.selfId());
    }

    /**
     * True if this node is a witness: it votes, stores the log and counts toward every
     * majority like any member, and it has no state machine. It answers no client, takes
     * no snapshot of anything and is not a place to read from.
     *
     * <p>What it is for: two data nodes and a witness tolerate the loss of any one of the
     * three, as three data nodes do, with the state stored twice instead of three times.
     *
     * <p>It does stand for election, later than a data node would
     * ({@link #WITNESS_ELECTION_FACTOR}), and that is not an oversight. An entry is
     * committed when two of three hold it, and those two are as often the leader and the
     * witness as the leader and the other data node. If the leader then fails, the
     * surviving data node lacks an entry the witness has, the witness rightly refuses it
     * its vote, and without a leader nothing would ever bring that entry across. So the
     * witness wins, replicates its log to the data node, and hands the lead over the
     * moment that node has caught up ({@link #handWitnessLeadershipOnLocked()}). While it
     * leads it refuses every client request.
     */
    public boolean isWitness() {
        return witness;
    }

    /** The refusal a witness gives every client-facing call: not here, and where instead if known. */
    private NotLeaderException witnessRefusalLocked() {
        String leader = currentLeaderId;
        return new NotLeaderException(config.selfId().equals(leader) ? leaderTransferTarget : leader);
    }

    /**
     * A leading witness passes the lead to the first data node that holds its whole log.
     * Called on every heartbeat and every acknowledged append; does nothing on any other
     * node. Caller must hold {@code lock}.
     */
    private void handWitnessLeadershipOnLocked() {
        if (!witness || role != ServerRole.LEADER || leaderTransferTarget != null) {
            return;
        }
        long lastLogIndex = store.getLastLogIndex();
        for (String peerId : currentConfiguration.keySet()) {
            if (peerId.equals(config.selfId()) || knownWitnesses.contains(peerId)) {
                continue;
            }
            if (matchIndex.getOrDefault(peerId, 0L) >= lastLogIndex && peerTransports.containsKey(peerId)) {
                log("witness: " + peerId + " holds the whole log, handing the lead to it");
                // The outcome needs no handling here: success ends this leadership, and
                // after a failure or a timeout the next heartbeat tries again.
                startLeadershipTransferLocked(peerId).exceptionally(failure -> null);
                return;
            }
        }
    }

    /** What a witness stores in place of a session table: the mark that its snapshots hold no state. */
    private static final byte[] WITNESS_SNAPSHOT_MARK =
            com.fbtberger.raft.proto.ClientSessionTable.newBuilder().setWitness(true).build().toByteArray();

    /**
     * A data directory that a witness has compacted holds the tail of the log and no
     * state for what came before. A node with a state machine that started from it would
     * come up with part of the state and report itself healthy.
     */
    private void refuseToStartFromAWitnessSnapshot(RaftStorage.SnapshotMeta snapshot) {
        if (witness || snapshot.sessionData.length == 0) {
            return;
        }
        boolean writtenByWitness;
        try {
            writtenByWitness = com.fbtberger.raft.proto.ClientSessionTable.parseFrom(snapshot.sessionData).getWitness();
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("corrupt client session table in snapshot", e);
        }
        if (writtenByWitness) {
            throw new IllegalStateException("the data directory of " + config.selfId()
                    + " was written by a witness: its log is compacted through index "
                    + snapshot.lastIncludedIndex + " and there is no state for those entries. "
                    + "Start this node as a witness (node.witness=true), or empty the directory "
                    + "and let it rejoin and catch up from the leader.");
        }
    }

    /**
     * Log compaction on a witness. There is no state to snapshot, so this only moves the
     * point the log starts at -- and it keeps {@link RaftConfig#witnessLogRetain()}
     * committed entries behind, because those entries are everything a witness can give a
     * data node that has fallen behind.
     *
     * @param force compact as far as the retained tail allows, whatever the threshold
     */
    private void compactWitnessLogLocked(boolean force) {
        long snapshotIndex = store.getSnapshotIndex();
        long target = lastApplied.get() - config.witnessLogRetain();
        if (target <= snapshotIndex || (!force && target - snapshotIndex < config.snapshotThreshold())) {
            return;
        }
        long targetTerm = store.getTermAt(target);
        // The configuration as of `target`, not the current one: entries after it stay in
        // the log, and one of them may be a configuration change that has yet to commit.
        byte[] configurationData = null;
        for (long i = target; i > snapshotIndex; i--) {
            LogEntry entry = store.getLogEntry(i);
            if (entry != null && entry.hasConfiguration()) {
                configurationData = entry.getConfiguration().toByteArray();
                break;
            }
        }
        if (configurationData == null) {
            RaftStorage.SnapshotMeta previous = store.getSnapshotMeta();
            configurationData = previous != null && previous.configurationData.length > 0
                    ? previous.configurationData
                    : toProto(config.peerAddresses()).toByteArray();
        }
        try (RaftStorage.SnapshotSink sink = store.beginSnapshot(target, targetTerm)) {
            sink.commit(configurationData, WITNESS_SNAPSHOT_MARK);
        } catch (IOException e) {
            logWarn("witness: could not compact the log through index " + target + ": " + e);
            return;
        }
        log("witness: log compacted through index " + target + "; "
                + config.witnessLogRetain() + " committed entries kept");
    }

    /**
     * This server is the leader and will not take the request now: more are already
     * waiting than {@code overload.max.pending.writes} / {@code .reads} allow. Nothing was
     * appended. Unlike {@link NotLeaderException} this is not a reason to try another
     * node -- the others are followers -- but to wait {@link #retryAfterMs} and ask again.
     *
     * <p>Why refuse at all: a command accepted here occupies memory and log until a
     * majority has stored it. When followers or the disk are slower than the clients,
     * accepting everything only moves the queue into this process, where it grows until
     * every client has timed out on a command that is still going to be applied. Refusing
     * at the door keeps the latency of what was accepted bounded and tells the client the
     * truth while it can still act on it.
     */
    public static final class OverloadedException extends RuntimeException {
        /** A first guess; the client stretches it on every further refusal. */
        public static final long RETRY_AFTER_MS = 50;
        public final long retryAfterMs = RETRY_AFTER_MS;

        public OverloadedException(String what, int waiting) {
            super("overloaded: " + waiting + " " + what + " already waiting; retry in " + RETRY_AFTER_MS + " ms");
        }
    }

    /** Whether the leader has as many commands waiting for their commit as it accepts. Counts the refusal. */
    private boolean overloadedWithWritesLocked() {
        int max = config.maxPendingWrites();
        if (max > 0 && pendingClientRequests.size() >= max) {
            metrics.clientOverloaded(true);
            return true;
        }
        return false;
    }

    private int pendingReadCount() {
        lock.lock();
        try {
            return pendingReadBarriers.size();
        } finally {
            lock.unlock();
        }
    }

    public static final class NotLeaderException extends RuntimeException {
        public final String leaderHint;

        public NotLeaderException(String leaderHint) {
            super("not the leader" + (leaderHint != null ? "; try " + leaderHint : ""));
            this.leaderHint = leaderHint;
        }
    }

    /**
     * Thrown by the configuration-change entry points ({@link #addServer}, {@link #removeServer},
     * {@link #addLearner}, {@link #removeLearner}, {@link #promoteLearner},
     * {@link #setConfiguration}) for requests that are invalid regardless of who the leader is.
     */
    /**
     * A tagged command at or below what its client already acknowledged. The client said
     * it has that answer, so the result is no longer held -- and applying the command
     * again is the one thing a session exists to prevent. Only a client that reuses a
     * sequence number sees this.
     */
    public static final class StaleClientRequestException extends RuntimeException {
        public StaleClientRequestException(String clientId, long sequence) {
            super("sequence " + sequence + " of client " + clientId
                    + " was already acknowledged; its result is no longer held");
        }
    }

    /** A follower read this server could not serve in time; another node can. */
    public static final class FollowerReadException extends RuntimeException {
        public FollowerReadException(String message) {
            super(message);
        }
    }

    public static final class ConfigurationChangeException extends RuntimeException {
        public ConfigurationChangeException(String message) {
            super(message);
        }
    }

    // ------------------------------------------------------------------
    // Cluster configuration bookkeeping (§6)
    // ------------------------------------------------------------------

    /**
     * Finds the latest configuration entry in our own log -- scanning
     * backwards, since we only ever need the most recent one -- and
     * installs it as the effective configuration. The scan stops at the
     * current snapshot boundary (§7), since anything at or before it has
     * been compacted out of the log; if nothing newer turns up there, falls
     * back to the configuration bundled inside the snapshot itself (also
     * persisted by {@code saveSnapshotAndCompact}, precisely so a §6
     * configuration entry can be compacted away without this getting lost).
     * Only with no snapshot and no configuration entry at all does this
     * fall back to the bootstrap configuration from the .properties file,
     * which is how every cluster starts out. Called once at startup, and
     * again after any log truncation that might have invalidated the
     * currently cached configuration.
     */
    private void recomputeEffectiveConfiguration() {
        long snapshotIndex = store.getSnapshotIndex();
        for (long i = store.getLastLogIndex(); i > snapshotIndex; i--) {
            LogEntry entry = store.getLogEntry(i);
            if (entry != null && entry.hasConfiguration()) {
                applyConfigurationLocked(i, entry.getConfiguration());
                return;
            }
        }
        RaftStorage.SnapshotMeta snapshot = store.getSnapshotMeta();
        if (snapshot != null && snapshot.configurationData.length > 0) {
            try {
                applyConfigurationLocked(snapshotIndex, ClusterConfiguration.parseFrom(snapshot.configurationData));
                return;
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalStateException("corrupt configuration in snapshot", e);
            }
        }
        // No configuration entry and no snapshot: fall back to the bootstrap
        // membership from the .properties file. A node started with
        // node.learner=true bootstraps itself as a non-voting learner (§4.2.1)
        // -- every other peer is a voting member and this node stays out of
        // the voting configuration until a committed entry promotes it, so it
        // never stands for election on its own.
        if (config.isLearner()) {
            Map<String, String> members = new LinkedHashMap<>(config.peerAddresses());
            String selfAddress = members.remove(config.selfId());
            Map<String, String> learners = new LinkedHashMap<>();
            if (selfAddress != null) learners.put(config.selfId(), selfAddress);
            applyConfigurationLocked(0, configProto(members, learners));
        } else {
            applyConfigurationLocked(0, toProto(config.peerAddresses()));
        }
    }

    /**
     * Installs {@code configuration} as the effective one and reconciles
     * our peer connections to match it: opens a stub for any newly added
     * member, and closes/drops the stub for any member that's no longer
     * present. Per §6, this happens the moment the entry exists in our
     * log -- not when it commits.
     */
    private void applyConfigurationLocked(long index, ClusterConfiguration configuration) {
        Map<String, String> members = new LinkedHashMap<>();
        for (ClusterConfiguration.Member member : configuration.getMembersList()) {
            members.put(member.getId(), member.getAddress());
        }
        if (configuration.getOldMembersCount() > 0) {
            Map<String, String> old = new LinkedHashMap<>();
            for (ClusterConfiguration.Member m : configuration.getOldMembersList()) {
                old.put(m.getId(), m.getAddress());
            }
            this.oldConfiguration = Map.copyOf(old);
        } else {
            this.oldConfiguration = null;
        }
        // §4.2.1: parse the non-voting learner set carried by this entry.
        Map<String, String> learners = new LinkedHashMap<>();
        for (ClusterConfiguration.Member m : configuration.getLearnersList()) {
            // Defensive: a server that is a voting member is never also a
            // learner, so drop any such overlap in favour of the voting role.
            if (!members.containsKey(m.getId())) {
                learners.put(m.getId(), m.getAddress());
            }
        }
        this.currentLearners = Map.copyOf(learners);
        this.currentConfiguration = Map.copyOf(members);
        this.currentConfigurationIndex = index;

        // The leader replicates to voters, joint-mode old voters AND learners
        // alike, so a transport must exist for every one of them.
        Map<String, String> allMembers = new LinkedHashMap<>(members);
        if (oldConfiguration != null) allMembers.putAll(oldConfiguration);
        allMembers.putAll(learners);

        for (Map.Entry<String, String> member : allMembers.entrySet()) {
            if (member.getKey().equals(config.selfId())) continue;
            peerTransports.computeIfAbsent(member.getKey(), id -> transportFactory.connect(member.getValue()));
        }

        List<String> noLongerMembers = new ArrayList<>();
        for (String peerId : peerTransports.keySet()) {
            if (!allMembers.containsKey(peerId)) {
                noLongerMembers.add(peerId);
            }
        }
        for (String peerId : noLongerMembers) {
            RaftTransport removed = peerTransports.remove(peerId);
            if (removed != null) removed.close();
            nextIndex.remove(peerId);
            matchIndex.remove(peerId);
            peerInflight.remove(peerId);
            snapshotTransfers.remove(peerId);
        }
    }

    private static ClusterConfiguration toProto(Map<String, String> members) {
        return configProto(members, Map.of());
    }

    /**
     * Builds a (non-joint) configuration entry carrying both the voting
     * {@code members} and the non-voting {@code learners} (§4.2.1). Every
     * leader-initiated configuration change routes through here so the learner
     * set is preserved across membership changes and log compaction rather
     * than being silently dropped.
     */
    private static ClusterConfiguration configProto(Map<String, String> members,
                                                     Map<String, String> learners) {
        ClusterConfiguration.Builder builder = ClusterConfiguration.newBuilder();
        addMembers(builder::addMembers, members);
        addMembers(builder::addLearners, learners);
        return builder.build();
    }

    private static ClusterConfiguration toJointProto(Map<String, String> oldMembers,
                                                      Map<String, String> newMembers,
                                                      Map<String, String> learners) {
        ClusterConfiguration.Builder builder = ClusterConfiguration.newBuilder();
        addMembers(builder::addMembers, newMembers);
        addMembers(builder::addOldMembers, oldMembers);
        addMembers(builder::addLearners, learners);
        return builder.build();
    }

    private static void addMembers(java.util.function.Consumer<ClusterConfiguration.Member> sink,
                                   Map<String, String> members) {
        for (Map.Entry<String, String> m : members.entrySet()) {
            sink.accept(ClusterConfiguration.Member.newBuilder()
                    .setId(m.getKey()).setAddress(m.getValue()).build());
        }
    }

    /**
     * True if {@code peerId} is a voting member of the effective configuration
     * (including the old configuration during a joint-consensus transition).
     * Learners return false. Used to keep learners out of every majority
     * decision.
     */
    private boolean isVotingMember(String peerId) {
        if (currentConfiguration.containsKey(peerId)) return true;
        Map<String, String> old = oldConfiguration;
        return old != null && old.containsKey(peerId);
    }

    /** The voting peers (current + joint old configuration), excluding self and learners. */
    private List<String> votingPeerIdsExcludingSelf() {
        java.util.LinkedHashSet<String> ids = new java.util.LinkedHashSet<>(currentConfiguration.keySet());
        Map<String, String> old = oldConfiguration;
        if (old != null) ids.addAll(old.keySet());
        ids.remove(config.selfId());
        return new ArrayList<>(ids);
    }

    /**
     * Majority size for the current configuration. During joint consensus
     * (C_old,new), returns the larger of the two majorities so that any
     * check using {@code count >= majority()} implicitly requires
     * majorities from both old and new configurations.
     */
    private int majority() {
        int newMajority = currentConfiguration.size() / 2 + 1;
        Map<String, String> old = oldConfiguration;
        if (old == null) return newMajority;
        int oldMajority = old.size() / 2 + 1;
        return Math.max(oldMajority, newMajority);
    }

    /**
     * Checks whether {@code count} satisfies separate majorities in both
     * the old and new configurations during joint consensus, or just the
     * current majority when not in joint mode. Used by
     * {@link #advanceCommitIndex()} for accurate commit decisions.
     */
    private boolean hasSeparateMajorities(java.util.function.ToLongFunction<String> matchFn, long n) {
        Map<String, String> old = oldConfiguration;
        int newCount = countMatches(currentConfiguration, matchFn, n);
        if (old == null) {
            return newCount >= currentConfiguration.size() / 2 + 1;
        }
        int oldCount = countMatches(old, matchFn, n);
        return newCount >= currentConfiguration.size() / 2 + 1
                && oldCount >= old.size() / 2 + 1;
    }

    private int countMatches(Map<String, String> config,
                             java.util.function.ToLongFunction<String> matchFn, long n) {
        int count = 0;
        for (String id : config.keySet()) {
            if (id.equals(this.config.selfId())) {
                if (leaderDiskMatchIndex.get() >= n) count++;
            } else {
                if (matchFn.applyAsLong(id) >= n) count++;
            }
        }
        return count;
    }

    private void log(String msg) {
        LOG.info("[{}] {}", config.selfId(), msg);
    }

    /** v101: replication problems belong at WARN — they used to be logged nowhere at all. */
    private void logWarn(String msg) {
        LOG.warn("[{}] {}", config.selfId(), msg);
    }

    /**
     * Like {@link #log(String)}, but for a Throwable — logs the full cause chain (SLF4J's
     * trailing-Throwable convention prints the complete stack trace, not just
     * {@code t.toString()}). Change 78/79: a bare {@code log("... " + t)} only ever showed
     * gRPC's generic wrapper (e.g. "UNAVAILABLE: io exception"), never the actual underlying
     * cause (a TLS handshake failure, hostname mismatch, etc.) buried in {@code t.getCause()} —
     * which made a real live TLS misconfiguration on a 3-node cluster look like an opaque,
     * undiagnosable network error for an extended live debugging session.
     */
    private void log(String msg, Throwable t) {
        LOG.warn("[{}] {}", config.selfId(), msg, t);
    }

    // ------------------------------------------------------------------
    // Snapshot chunking helpers (§7, Figure 13)
    // ------------------------------------------------------------------

    /**
     * One snapshot on its way to one follower. Holds where the transfer stands and the
     * few bytes that frame the payload -- never the payload, which stays in the storage
     * backend and is read a chunk at a time.
     *
     * <p>What goes over the wire is the stream {@link #packSnapshotData} describes:
     * {@code header | payload | trailer}.
     */
    static final class SnapshotTransfer {
        final long lastIncludedIndex;
        final long lastIncludedTerm;
        final byte[] header;
        final long payloadLength;
        final byte[] trailer;
        final long totalLength;
        long nextOffset;
        boolean inFlight;

        SnapshotTransfer(RaftStorage.SnapshotMeta snapshot) {
            this.lastIncludedIndex = snapshot.lastIncludedIndex;
            this.lastIncludedTerm = snapshot.lastIncludedTerm;
            this.payloadLength = snapshot.dataLength;
            this.header = snapshotHeader((int) snapshot.dataLength, snapshot.sessionData.length > 0);
            this.trailer = snapshotTrailer(snapshot.configurationData, snapshot.sessionData);
            this.totalLength = header.length + payloadLength + trailer.length;
        }

        /**
         * The {@code length} bytes of the stream starting at {@code offset}, or null if the
         * stored snapshot is no longer this one.
         */
        byte[] read(RaftStorage store, long offset, int length) throws IOException {
            byte[] chunk = new byte[length];
            int filled = 0;
            while (filled < length) {
                long at = offset + filled;
                int want = length - filled;
                if (at < header.length) {
                    int n = (int) Math.min(want, header.length - at);
                    System.arraycopy(header, (int) at, chunk, filled, n);
                    filled += n;
                } else if (at < header.length + payloadLength) {
                    long payloadAt = at - header.length;
                    int n = (int) Math.min(want, payloadLength - payloadAt);
                    int read = store.readSnapshotData(lastIncludedIndex, payloadAt, chunk, filled, n);
                    if (read < 0) {
                        return null;
                    }
                    if (read != n) {
                        throw new IOException("snapshot " + lastIncludedIndex + " is shorter than recorded: "
                                + read + " of " + n + " bytes at offset " + payloadAt);
                    }
                    filled += n;
                } else {
                    long trailerAt = at - header.length - payloadLength;
                    int n = (int) Math.min(want, trailer.length - trailerAt);
                    System.arraycopy(trailer, (int) trailerAt, chunk, filled, n);
                    filled += n;
                }
            }
            return chunk;
        }
    }

    /**
     * The receiving end of {@link SnapshotTransfer}: takes the stream apart as it arrives.
     * The payload goes to the storage backend's sink as it comes; only the header and the
     * trailer, a few bytes of configuration and session table, are held here.
     */
    private final class SnapshotReceiver implements AutoCloseable {
        final long lastIncludedIndex;
        final long lastIncludedTerm;
        long expectedOffset;

        private final byte[] header = new byte[Integer.BYTES];
        private int headerFilled;
        private boolean hasSessions;
        private long payloadLength;
        private long payloadWritten;
        private RaftStorage.SnapshotSink sink;
        private final ByteArrayOutputStream trailer = new ByteArrayOutputStream();

        SnapshotReceiver(long lastIncludedIndex, long lastIncludedTerm) {
            this.lastIncludedIndex = lastIncludedIndex;
            this.lastIncludedTerm = lastIncludedTerm;
        }

        void accept(ByteString data) throws IOException {
            int position = 0;
            int size = data.size();
            while (position < size && headerFilled < header.length) {
                header[headerFilled++] = data.byteAt(position++);
            }
            if (headerFilled == header.length && sink == null) {
                int head = readInt(header, 0);
                hasSessions = (head & SNAPSHOT_HAS_SESSIONS) != 0;
                payloadLength = head & ~SNAPSHOT_HAS_SESSIONS;
                sink = store.beginSnapshot(lastIncludedIndex, lastIncludedTerm);
            }
            if (sink != null && position < size) {
                int toPayload = (int) Math.min(size - position, payloadLength - payloadWritten);
                if (toPayload > 0 && witness) {
                    // Counted and dropped. A witness keeps the boundary the snapshot
                    // sets -- index, term, configuration -- and none of the state.
                    payloadWritten += toPayload;
                    position += toPayload;
                } else if (toPayload > 0) {
                    data.substring(position, position + toPayload).writeTo(sink.stream());
                    payloadWritten += toPayload;
                    position += toPayload;
                }
                if (position < size) {
                    data.substring(position).writeTo(trailer);
                }
            }
            expectedOffset += size;
        }

        /** Makes the received snapshot the stored one and returns its session table. */
        byte[] commit() throws IOException {
            if (sink == null || payloadWritten != payloadLength) {
                throw new IOException("snapshot " + lastIncludedIndex + " ended after " + payloadWritten
                        + " of " + payloadLength + " payload bytes");
            }
            byte[] rest = trailer.toByteArray();
            byte[] configurationData;
            byte[] sessionData;
            if (hasSessions) {
                int configLength = readInt(rest, 0);
                configurationData = Arrays.copyOfRange(rest, Integer.BYTES, Integer.BYTES + configLength);
                sessionData = Arrays.copyOfRange(rest, Integer.BYTES + configLength, rest.length);
            } else {
                configurationData = rest;
                sessionData = new byte[0];
            }
            try (RaftStorage.SnapshotSink committing = sink) {
                committing.commit(configurationData, witness ? WITNESS_SNAPSHOT_MARK : sessionData);
            }
            return witness ? new byte[0] : sessionData;
        }

        @Override
        public void close() {
            if (sink != null) {
                sink.close();
            }
        }
    }

    /** Drops a half-received snapshot and the temporary file behind it. Caller must hold {@code lock}. */
    private void discardSnapshotReceiverLocked() {
        if (snapshotReceiver != null) {
            snapshotReceiver.close();
            snapshotReceiver = null;
        }
    }

    /** Replaces the state machine's state with the stored snapshot's, read as a stream. */
    private void restoreStateMachineFromStoredSnapshot() {
        try (java.io.InputStream in = store.openSnapshotData()) {
            if (in == null) {
                throw new IllegalStateException("the storage backend reports a snapshot and has no data for it");
            }
            stateMachines.restoreSnapshot(in);
        } catch (IOException e) {
            throw new UncheckedIOException("could not read the stored snapshot", e);
        }
    }

    /** The four bytes that open the stream: payload length, top bit set if a session table follows. */
    static byte[] snapshotHeader(int payloadLength, boolean withSessions) {
        byte[] header = new byte[Integer.BYTES];
        writeInt(header, 0, withSessions ? payloadLength | SNAPSHOT_HAS_SESSIONS : payloadLength);
        return header;
    }

    /** What follows the payload: the configuration, length-prefixed and followed by the table if there is one. */
    static byte[] snapshotTrailer(byte[] configurationData, byte[] sessionData) {
        if (sessionData.length == 0) {
            return configurationData;
        }
        byte[] trailer = new byte[Integer.BYTES + configurationData.length + sessionData.length];
        writeInt(trailer, 0, configurationData.length);
        System.arraycopy(configurationData, 0, trailer, Integer.BYTES, configurationData.length);
        System.arraycopy(sessionData, 0, trailer, Integer.BYTES + configurationData.length, sessionData.length);
        return trailer;
    }

    /**
     * Packs stateMachineData and configurationData into a single byte
     * stream for chunked transfer: {@code [4-byte big-endian smLen][smData][cfgData]}.
     */
    static byte[] packSnapshotData(byte[] stateMachineData, byte[] configurationData) {
        return packSnapshotData(stateMachineData, configurationData, new byte[0]);
    }

    /** Top bit of the leading length word: a session table follows the configuration. */
    private static final int SNAPSHOT_HAS_SESSIONS = 0x8000_0000;

    /**
     * As above, plus the client session table. With an empty table the bytes are exactly
     * the two-payload format, so a cluster in which nobody uses sessions keeps exchanging
     * snapshots with nodes that predate them. With a table the layout is
     * {@code [smLen | top bit][smData][4-byte cfgLen][cfgData][sessionData]}; the top bit
     * is free because a length is never negative.
     */
    static byte[] packSnapshotData(byte[] stateMachineData, byte[] configurationData, byte[] sessionData) {
        byte[] header = snapshotHeader(stateMachineData.length, sessionData.length > 0);
        byte[] trailer = snapshotTrailer(configurationData, sessionData);
        byte[] packed = new byte[header.length + stateMachineData.length + trailer.length];
        System.arraycopy(header, 0, packed, 0, header.length);
        System.arraycopy(stateMachineData, 0, packed, header.length, stateMachineData.length);
        System.arraycopy(trailer, 0, packed, header.length + stateMachineData.length, trailer.length);
        return packed;
    }

    private static void writeInt(byte[] target, int at, int value) {
        target[at] = (byte) (value >>> 24);
        target[at + 1] = (byte) (value >>> 16);
        target[at + 2] = (byte) (value >>> 8);
        target[at + 3] = (byte) value;
    }

    private static int readInt(byte[] source, int at) {
        return ((source[at] & 0xFF) << 24) | ((source[at + 1] & 0xFF) << 16)
                | ((source[at + 2] & 0xFF) << 8) | (source[at + 3] & 0xFF);
    }

    /**
     * Inverse of {@link #packSnapshotData}: splits a reassembled byte stream
     * back into the two original payloads and wraps them in a {@link RaftStorage.Snapshot}.
     */
    static RaftStorage.Snapshot unpackSnapshotData(long lastIncludedIndex, long lastIncludedTerm, byte[] packed) {
        int head = readInt(packed, 0);
        int smLen = head & ~SNAPSHOT_HAS_SESSIONS;
        byte[] smData = Arrays.copyOfRange(packed, 4, 4 + smLen);
        if ((head & SNAPSHOT_HAS_SESSIONS) == 0) {
            byte[] cfgData = Arrays.copyOfRange(packed, 4 + smLen, packed.length);
            return new RaftStorage.Snapshot(lastIncludedIndex, lastIncludedTerm, smData, cfgData);
        }
        int cfgStart = 4 + smLen + 4;
        int cfgLen = readInt(packed, 4 + smLen);
        byte[] cfgData = Arrays.copyOfRange(packed, cfgStart, cfgStart + cfgLen);
        byte[] sessionData = Arrays.copyOfRange(packed, cfgStart + cfgLen, packed.length);
        return new RaftStorage.Snapshot(lastIncludedIndex, lastIncludedTerm, smData, cfgData, sessionData);
    }
}
