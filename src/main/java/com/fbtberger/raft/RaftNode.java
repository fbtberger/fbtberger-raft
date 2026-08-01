/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.ClusterConfiguration;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.LogEntry;
import com.fbtberger.raft.proto.PreVoteRequest;
import com.fbtberger.raft.proto.PreVoteResponse;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import com.fbtberger.raft.transport.RaftTransport;
import com.fbtberger.raft.transport.RaftTransportFactory;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
    // §10.2.2: cap each AppendEntries batch so heartbeats aren't starved.
    private static final int MAX_BATCH_BYTES = 1_048_576; // 1 MB
    // §10.2.2: maximum pipelined AppendEntries RPCs in flight per peer.
    private static final int MAX_INFLIGHT_APPENDS = 2;

    private final RaftConfig config;
    private final RaftStorage store;
    private final StateMachine stateMachine;
    private final RaftTransportFactory transportFactory;
    private final RaftMetrics metrics;
    private final Map<String, RaftTransport> peerTransports = new ConcurrentHashMap<>(); // excludes self
    private final ScheduledExecutorService scheduler;

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

    // Leader-side: in-progress chunked snapshot transfers per peer (§7).
    private final Map<String, SnapshotTransfer> snapshotTransfers = new ConcurrentHashMap<>();

    // Follower-side: buffer for reassembling incoming snapshot chunks.
    private ByteArrayOutputStream pendingSnapshotBuffer;
    private long pendingSnapshotIndex;
    private long pendingSnapshotTerm;
    private long pendingSnapshotExpectedOffset;

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
    private volatile boolean snapshotInProgress = false;

    // §4.2.3 PreVote: timestamp of the last valid leader contact (heartbeat
    // or AppendEntries). A server only grants a PreVote if it hasn't heard
    // from a leader within the election timeout window.
    private volatile long lastLeaderContactMs = 0;

    // §3.10: ongoing leadership transfer target, or null if none.
    private volatile String leaderTransferTarget = null;
    private volatile CompletableFuture<Void> leaderTransferResult = null;
    private volatile ScheduledFuture<?> leaderTransferTimeout = null;

    // Linearizable reads (ReadIndex / Lease): pending read barriers awaiting
    // leadership confirmation from a majority of peers.
    private final List<ReadBarrier> pendingReadBarriers = new ArrayList<>();
    // Lease-based reads: per-peer timestamp of last successful AppendEntries ack.
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

    public RaftNode(RaftConfig config,
                     RaftStorage store,
                     StateMachine stateMachine,
                     RaftTransportFactory transportFactory,
                     RaftMetrics metrics) {
        this.config = config;
        this.store = store;
        this.stateMachine = stateMachine;
        this.transportFactory = transportFactory;
        this.metrics = metrics;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "raft-" + config.selfId());
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
        RaftStorage.Snapshot existingSnapshot = store.getSnapshot();
        if (existingSnapshot != null) {
            stateMachine.restoreSnapshot(existingSnapshot.stateMachineData);
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
    }

    /** Every server starts out as a follower (§5.1). */
    public void start() {
        started = true;
        log("starting as FOLLOWER, currentTerm=" + store.getCurrentTerm()
                + ", lastLogIndex=" + store.getLastLogIndex()
                + ", snapshotIndex=" + store.getSnapshotIndex()
                + ", configuration=" + currentConfiguration.keySet());
        // v122: treat startup as leader contact for stickiness purposes. The field initialises to
        // 0, i.e. "last heard from a leader in 1970", so a node that had only just come up granted
        // (pre-)votes to anyone before it had any chance to hear from the incumbent. During a
        // rolling restart that is the second grant. This does NOT delay our own campaign -- the
        // election timer is driven by resetElectionTimer() below, not by this field -- it only
        // stops us from unseating a leader we have not even listened for yet.
        lastLeaderContactMs = System.currentTimeMillis();
        resetElectionTimer();
    }

    public void shutdown() {
        scheduler.shutdownNow();
        for (RaftTransport transport : peerTransports.values()) {
            transport.close();
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

            AtomicLong preVotesGranted = new AtomicLong(1); // count self
            try {
                // §4.2.1: solicit (Pre)Votes only from voting members -- never
                // from learners, which do not participate in elections.
                for (String peerId : votingPeerIdsExcludingSelf()) {
                    RaftTransport peer = peerTransports.get(peerId);
                    if (peer == null) continue;
                    peer.preVote(preVoteRequest).whenComplete((response, t) -> {
                        if (t == null) {
                            handlePreVoteResponse(proposedTerm, response, preVotesGranted);
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

    private void handlePreVoteResponse(long proposedTerm, PreVoteResponse response, AtomicLong preVotesGranted) {
        lock.lock();
        try {
            if (store.getCurrentTerm() + 1 != proposedTerm) {
                return;
            }
            if (response.getVoteGranted() && preVotesGranted.incrementAndGet() >= majority()) {
                startRealElection(proposedTerm);
            }
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

        AtomicLong votesGranted = new AtomicLong(1);
        // §4.2.1: request votes only from voting members, never from learners.
        for (String peerId : votingPeerIdsExcludingSelf()) {
            RaftTransport peer = peerTransports.get(peerId);
            if (peer == null) continue;
            peer.requestVote(request).whenComplete((response, t) -> {
                if (t == null) handleRequestVoteResponse(newTerm, response, votesGranted);
            });
        }
    }

    private void handleRequestVoteResponse(long electionTerm, RequestVoteResponse response, AtomicLong votesGranted) {
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
                if (votesGranted.incrementAndGet() >= majority()) {
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

        for (Map.Entry<String, String> member : currentConfiguration.entrySet()) {
            String id = member.getKey();
            if (id.equals(config.selfId())) continue;
            appendPeerStatus(sb, id, lastLogIndex, "voter");
        }
        for (Map.Entry<String, String> learner : currentLearners.entrySet()) {
            appendPeerStatus(sb, learner.getKey(), lastLogIndex, "learner");
        }
        log(sb.toString());
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
                  : (System.currentTimeMillis() - lastAck) + "ms");
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
        if (leaderTransferTimeout != null) {
            leaderTransferTimeout.cancel(false);
            leaderTransferTimeout = null;
        }
        for (ReadBarrier rb : pendingReadBarriers) {
            rb.future.completeExceptionally(new NotLeaderException(null));
        }
        pendingReadBarriers.clear();
        oldConfiguration = null;
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
                                ? "self" : (System.currentTimeMillis() - lastLeaderContactMs) + "ms")
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
        if (role == ServerRole.LEADER) return true;
        return System.currentTimeMillis() - lastLeaderContactMs < ELECTION_TIMEOUT_MIN_MS;
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
                            ? "self" : (System.currentTimeMillis() - lastLeaderContactMs) + "ms")
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
                    lastLeaderContactMs = System.currentTimeMillis();
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
            if (request.getLeaderCommit() > commitIndex.get()) {
                commitIndex.set(Math.min(request.getLeaderCommit(), lastNewIndex));
                applyCommittedEntries();
            }

            lastLeaderContactMs = System.currentTimeMillis();
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
            lastLeaderContactMs = System.currentTimeMillis();
            resetElectionTimer();

            if (request.getLastIncludedIndex() <= store.getSnapshotIndex()) {
                return InstallSnapshotResponse.newBuilder().setTerm(currentTerm).build();
            }

            // Start a fresh buffer on offset 0 or when the snapshot identity changes.
            if (request.getOffset() == 0
                    || pendingSnapshotBuffer == null
                    || pendingSnapshotIndex != request.getLastIncludedIndex()) {
                pendingSnapshotBuffer = new ByteArrayOutputStream();
                pendingSnapshotIndex = request.getLastIncludedIndex();
                pendingSnapshotTerm = request.getLastIncludedTerm();
                pendingSnapshotExpectedOffset = 0;
            }

            if (request.getOffset() != pendingSnapshotExpectedOffset) {
                pendingSnapshotBuffer = null;
                return InstallSnapshotResponse.newBuilder().setTerm(currentTerm).build();
            }

            byte[] chunkData = request.getData().toByteArray();
            pendingSnapshotBuffer.write(chunkData, 0, chunkData.length);
            pendingSnapshotExpectedOffset += chunkData.length;
            metrics.snapshotChunkReceived();

            if (request.getDone()) {
                byte[] packed = pendingSnapshotBuffer.toByteArray();
                pendingSnapshotBuffer = null;

                RaftStorage.Snapshot snapshot = unpackSnapshotData(
                        pendingSnapshotIndex, pendingSnapshotTerm, packed);

                store.saveSnapshotAndCompact(snapshot);
                stateMachine.restoreSnapshot(snapshot.stateMachineData);

                if (snapshot.lastIncludedIndex > commitIndex.get()) {
                    commitIndex.set(snapshot.lastIncludedIndex);
                }
                lastApplied.set(Math.max(lastApplied.get(), snapshot.lastIncludedIndex));
                recomputeEffectiveConfiguration();

                metrics.snapshotInstalled();
                log("installed snapshot through index " + snapshot.lastIncludedIndex
                        + " (term " + snapshot.lastIncludedTerm + ") from " + request.getLeaderId());
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
            logThrottled(peerId, "kein Transport vorhanden — dieser Peer wird NICHT repliziert");
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
                handleAppendEntriesResponse(peerId, currentTerm, lastSentIndex, response);
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
        SnapshotTransfer transfer = snapshotTransfers.get(peerId);

        if (transfer != null && transfer.lastIncludedIndex < store.getSnapshotIndex()) {
            snapshotTransfers.remove(peerId);
            transfer = null;
        }

        if (transfer == null) {
            RaftStorage.Snapshot snapshot = store.getSnapshot();
            if (snapshot == null) {
                return;
            }
            byte[] packed = packSnapshotData(snapshot.stateMachineData, snapshot.configurationData);
            transfer = new SnapshotTransfer(snapshot.lastIncludedIndex, snapshot.lastIncludedTerm, packed);
            snapshotTransfers.put(peerId, transfer);
        }

        if (transfer.inFlight) {
            return;
        }

        int offset = (int) transfer.nextOffset;
        int chunkSize = config.snapshotChunkSize();
        int remaining = transfer.data.length - offset;
        int len = Math.min(chunkSize, remaining);
        boolean done = (offset + len >= transfer.data.length);

        long currentTerm = store.getCurrentTerm();
        InstallSnapshotRequest request = InstallSnapshotRequest.newBuilder()
                .setTerm(currentTerm)
                .setLeaderId(config.selfId())
                .setLastIncludedIndex(transfer.lastIncludedIndex)
                .setLastIncludedTerm(transfer.lastIncludedTerm)
                .setOffset(offset)
                .setData(ByteString.copyFrom(transfer.data, offset, len))
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

    private void handleAppendEntriesResponse(String peerId, long sentTerm, long lastSentIndex, AppendEntriesResponse response) {
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
                peerLastAckMs.put(peerId, System.currentTimeMillis());
                // §4.2.1: only a voting member's acknowledgement counts toward
                // the leadership-confirmation majority behind a ReadIndex; a
                // learner's ack must never help satisfy a read barrier.
                if (isVotingMember(peerId)) {
                    for (ReadBarrier rb : pendingReadBarriers) rb.confirm(peerId);
                }
                checkReadBarriers();
                if (peerId.equals(leaderTransferTarget)) {
                    checkTransferReadyLocked();
                }
            } else {
                metrics.replicationFailure();
                // §10.2.2 + v118: a rejection proves the follower does NOT hold the entry at the
                // prevLogIndex we sent, so any matchIndex we recorded at/above it is a lie. Normally
                // impossible — but a wiped/rebuilt peer truncates its log back to 0 while the leader
                // still holds its pre-wipe (high) matchIndex. Clamping nextIndex to matchIndex+1 then
                // pins it above snapshotIndex forever and replicateTo never switches to InstallSnapshot
                // (the "learner stuck at 0, leader silent" tail that outlives the v117 transport fix).
                // Use the follower's reported last index to pull BOTH pointers down — monotonically,
                // since a rejection can only ever reveal the follower has less than we assumed. Capped
                // at lastSentIndex so nextIndex always drops strictly below the index that just failed.
                long hint = response.getConflictLastLogIndex();
                matchIndex.merge(peerId, hint, (cur, h) -> Math.min(cur, h));
                nextIndex.put(peerId, Math.max(1, Math.min(hint + 1, lastSentIndex)));
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
            if (entry.hasConfiguration()) {
                result = new byte[0];
                ClusterConfiguration cfg = entry.getConfiguration();
                boolean isJoint = cfg.getOldMembersCount() > 0;
                boolean stillAMember = cfg.getMembersList().stream()
                        .anyMatch(member -> member.getId().equals(config.selfId()));
                if (role == ServerRole.LEADER && isJoint) {
                    log("joint config C_old,new committed; proposing final C_new");
                    appendAndReplicateLocked(LogEntry.newBuilder()
                            .setConfiguration(configProto(currentConfiguration, currentLearners)));
                }
                if (role == ServerRole.LEADER && !stillAMember && !isJoint) {
                    log("stepping down: committed configuration no longer includes us");
                    becomeFollowerLocked(store.getCurrentTerm());
                }
            } else {
                result = stateMachine.apply(entry.getCommand().toByteArray());
            }
            metrics.entryApplied();
            CompletableFuture<byte[]> pending = pendingClientRequests.remove(index);
            if (pending != null) {
                pending.complete(result);
            }
        }
        maybeTakeSnapshotLocked();
    }

    // ------------------------------------------------------------------
    // Log compaction / COW snapshot isolation (§7, §5.1)
    // ------------------------------------------------------------------

    /** Takes a new snapshot if enough newly applied entries have piled up since the last one. Caller must hold {@code lock}. */
    private void maybeTakeSnapshotLocked() {
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
        java.util.function.Supplier<byte[]> cowSnapshot = captureCowSnapshot();
        byte[] configurationData = configProto(currentConfiguration, currentLearners).toByteArray();

        snapshotInProgress = true;
        scheduler.execute(() -> {
            try {
                if (applied <= store.getSnapshotIndex()) {
                    return;
                }
                byte[] stateMachineData = cowSnapshot.get();
                RaftStorage.Snapshot snapshot = new RaftStorage.Snapshot(
                        applied, includedTerm, stateMachineData, configurationData);
                store.saveSnapshotAndCompact(snapshot);
                metrics.snapshotTaken();
                log("snapshotted through index " + snapshot.lastIncludedIndex
                        + " (term " + snapshot.lastIncludedTerm
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
    private java.util.function.Supplier<byte[]> captureCowSnapshot() {
        return stateMachine.prepareCowSnapshot();
    }

    /** Synchronous snapshot for {@link #snapshotNow()} — blocks until complete. */
    private void takeSnapshotSync() {
        long applied = lastApplied.get();
        if (applied <= store.getSnapshotIndex()) {
            return;
        }
        long includedTerm = store.getTermAt(applied);
        byte[] stateMachineData = stateMachine.takeSnapshot();
        byte[] configurationData = configProto(currentConfiguration, currentLearners).toByteArray();
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(applied, includedTerm, stateMachineData, configurationData));
        metrics.snapshotTaken();
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
    // Client interaction (§8, simplified: no per-client serial-number
    // de-duplication, so a retried command can in principle be applied
    // twice -- see the README for what a production system would add here)
    // ------------------------------------------------------------------

    /**
     * Appends a client command to the leader's log and kicks off
     * replication. A non-leader fails fast with a hint about who the
     * leader is, mirroring how §8 describes clients being redirected.
     */
    public CompletableFuture<byte[]> submitCommand(byte[] command) {
        lock.lock();
        try {
            if (role != ServerRole.LEADER) {
                metrics.clientRejected();
                return failedFuture(new NotLeaderException(currentLeaderId));
            }
            if (leaderTransferTarget != null) {
                metrics.clientRejected();
                return failedFuture(new NotLeaderException(leaderTransferTarget));
            }
            return appendAndReplicateLocked(LogEntry.newBuilder().setCommand(ByteString.copyFrom(command)));
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
            if (role != ServerRole.LEADER) {
                CompletableFuture<Long> f = new CompletableFuture<>();
                f.completeExceptionally(new NotLeaderException(currentLeaderId));
                return f;
            }
            long ri = commitIndex.get();
            if (majority() == 1) {
                return lastApplied.get() >= ri
                        ? CompletableFuture.completedFuture(ri)
                        : awaitApplied(ri).thenApply(v -> ri);
            }
            ReadBarrier barrier = new ReadBarrier(ri);
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
            if (role != ServerRole.LEADER) {
                CompletableFuture<Void> f = new CompletableFuture<>();
                f.completeExceptionally(new NotLeaderException(currentLeaderId));
                return f;
            }
            if (majority() == 1 || hasValidLease()) {
                long ri = commitIndex.get();
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
        long now = System.currentTimeMillis();
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
            if (lastApplied.get() >= targetIndex) {
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
        final CompletableFuture<Long> future = new CompletableFuture<>();
        final java.util.Set<String> confirmed = ConcurrentHashMap.newKeySet();

        ReadBarrier(long readIndex) {
            this.readIndex = readIndex;
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

            leaderTransferTarget = targetId;
            leaderTransferResult = result;
            log("starting leadership transfer to " + targetId);

            leaderTransferTimeout = scheduler.schedule(() -> {
                lock.lock();
                try {
                    if (leaderTransferTarget != null) {
                        log("leadership transfer to " + leaderTransferTarget + " timed out, aborting");
                        leaderTransferTarget = null;
                        leaderTransferTimeout = null;
                        leaderTransferResult = null;
                        result.completeExceptionally(new RuntimeException("leadership transfer timed out"));
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

    private void checkTransferReadyLocked() {
        String targetId = leaderTransferTarget;
        CompletableFuture<Void> result = leaderTransferResult;
        if (targetId == null || result == null) return;

        long targetMatch = matchIndex.getOrDefault(targetId, 0L);
        if (targetMatch < store.getLastLogIndex()) return;

        RaftTransport transport = peerTransports.get(targetId);
        if (transport == null) return;

        log("target " + targetId + " is caught up, sending TimeoutNow");
        long currentTerm = store.getCurrentTerm();
        transport.timeoutNow(TimeoutNowRequest.newBuilder()
                .setTerm(currentTerm).build()).whenComplete((resp, t) -> {
            lock.lock();
            try {
                leaderTransferTarget = null;
                leaderTransferResult = null;
                if (leaderTransferTimeout != null) {
                    leaderTransferTimeout.cancel(false);
                    leaderTransferTimeout = null;
                }
                if (t != null) {
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
            log("received TimeoutNow, starting immediate election");
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
        RaftStorage.Snapshot snapshot = store.getSnapshot();
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

    static final class SnapshotTransfer {
        final long lastIncludedIndex;
        final long lastIncludedTerm;
        final byte[] data;
        long nextOffset;
        boolean inFlight;

        SnapshotTransfer(long lastIncludedIndex, long lastIncludedTerm, byte[] data) {
            this.lastIncludedIndex = lastIncludedIndex;
            this.lastIncludedTerm = lastIncludedTerm;
            this.data = data;
        }
    }

    /**
     * Packs stateMachineData and configurationData into a single byte
     * stream for chunked transfer: {@code [4-byte big-endian smLen][smData][cfgData]}.
     */
    static byte[] packSnapshotData(byte[] stateMachineData, byte[] configurationData) {
        byte[] packed = new byte[4 + stateMachineData.length + configurationData.length];
        packed[0] = (byte) (stateMachineData.length >>> 24);
        packed[1] = (byte) (stateMachineData.length >>> 16);
        packed[2] = (byte) (stateMachineData.length >>> 8);
        packed[3] = (byte) (stateMachineData.length);
        System.arraycopy(stateMachineData, 0, packed, 4, stateMachineData.length);
        System.arraycopy(configurationData, 0, packed, 4 + stateMachineData.length, configurationData.length);
        return packed;
    }

    /**
     * Inverse of {@link #packSnapshotData}: splits a reassembled byte stream
     * back into the two original payloads and wraps them in a {@link RaftStorage.Snapshot}.
     */
    static RaftStorage.Snapshot unpackSnapshotData(long lastIncludedIndex, long lastIncludedTerm, byte[] packed) {
        int smLen = ((packed[0] & 0xFF) << 24) | ((packed[1] & 0xFF) << 16)
                  | ((packed[2] & 0xFF) << 8)  | (packed[3] & 0xFF);
        byte[] smData = Arrays.copyOfRange(packed, 4, 4 + smLen);
        byte[] cfgData = Arrays.copyOfRange(packed, 4 + smLen, packed.length);
        return new RaftStorage.Snapshot(lastIncludedIndex, lastIncludedTerm, smData, cfgData);
    }
}
