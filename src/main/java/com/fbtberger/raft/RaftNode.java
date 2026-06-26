package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.ClusterConfiguration;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.LogEntry;
import com.fbtberger.raft.proto.RaftServiceGrpc;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannel;

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
import java.util.function.Function;

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
public final class RaftNode {

    private static final int HEARTBEAT_INTERVAL_MS = 50;
    private static final int ELECTION_TIMEOUT_MIN_MS = 150;
    private static final int ELECTION_TIMEOUT_MAX_MS = 300;

    private final RaftConfig config;
    private final RaftStorage store;
    private final StateMachine stateMachine;
    private final Function<String, RaftServiceGrpc.RaftServiceFutureStub> peerStubFactory;
    private final Map<String, RaftServiceGrpc.RaftServiceFutureStub> peerStubs = new ConcurrentHashMap<>(); // excludes self
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

    private final Random random = new Random();
    private ScheduledFuture<?> electionTimer;
    private ScheduledFuture<?> heartbeatTask;
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
                     Function<String, RaftServiceGrpc.RaftServiceFutureStub> peerStubFactory) {
        this.config = config;
        this.store = store;
        this.stateMachine = stateMachine;
        this.peerStubFactory = peerStubFactory;
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
    }

    /** Every server starts out as a follower (§5.1). */
    public void start() {
        started = true;
        log("starting as FOLLOWER, currentTerm=" + store.getCurrentTerm()
                + ", lastLogIndex=" + store.getLastLogIndex()
                + ", snapshotIndex=" + store.getSnapshotIndex()
                + ", configuration=" + currentConfiguration.keySet());
        resetElectionTimer();
    }

    public void shutdown() {
        scheduler.shutdownNow();
        for (RaftServiceGrpc.RaftServiceFutureStub stub : peerStubs.values()) {
            closeStubChannel(stub);
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
        int timeoutMs = ELECTION_TIMEOUT_MIN_MS
                + random.nextInt(ELECTION_TIMEOUT_MAX_MS - ELECTION_TIMEOUT_MIN_MS + 1);
        electionTimer = scheduler.schedule(this::startElection, timeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Starts a new election: bump the term, vote for ourselves, and ask every other server for its vote (§5.2). */
    private void startElection() {
        lock.lock();
        try {
            if (!currentConfiguration.containsKey(config.selfId())) {
                // §6: our own log already reflects a configuration that no
                // longer includes us (we were removed). A server in that
                // state must not try to become leader of a cluster it's no
                // longer part of -- just keep waiting.
                resetElectionTimer();
                return;
            }
            role = ServerRole.CANDIDATE;
            currentLeaderId = null;
            long newTerm = store.getCurrentTerm() + 1;
            store.setTermAndVote(newTerm, config.selfId());
            log("election timeout -> starting election for term " + newTerm);

            if (majority() == 1) {
                becomeLeaderLocked();
                return;
            }
            resetElectionTimer();

            RequestVoteRequest request = RequestVoteRequest.newBuilder()
                    .setTerm(newTerm)
                    .setCandidateId(config.selfId())
                    .setLastLogIndex(store.getLastLogIndex())
                    .setLastLogTerm(store.getLastLogTerm())
                    .build();

            AtomicLong votesGranted = new AtomicLong(1); // we vote for ourselves
            for (Map.Entry<String, RaftServiceGrpc.RaftServiceFutureStub> peer : peerStubs.entrySet()) {
                ListenableFuture<RequestVoteResponse> future = peer.getValue().requestVote(request);
                Futures.addCallback(future, new FutureCallback<RequestVoteResponse>() {
                    @Override
                    public void onSuccess(RequestVoteResponse response) {
                        handleRequestVoteResponse(newTerm, response, votesGranted);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        // Peer unreachable; the election will simply time
                        // out and we'll retry if no one has won (§5.2,
                        // outcome (c)).
                    }
                }, MoreExecutors.directExecutor());
            }
        } finally {
            lock.unlock();
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
        log("elected LEADER for term " + store.getCurrentTerm());
        if (electionTimer != null) electionTimer.cancel(false);
        if (heartbeatTask != null) heartbeatTask.cancel(false);

        // §8: commit a blank no-op entry for our new term right away. Until
        // an entry from our own term has committed, we can't be sure which
        // older entries are actually committed yet, even though Leader
        // Completeness guarantees we already have them in our log.
        long noOpIndex = store.getLastLogIndex() + 1;
        LogEntry noOp = LogEntry.newBuilder()
                .setIndex(noOpIndex)
                .setTerm(store.getCurrentTerm())
                .setCommand(ByteString.EMPTY)
                .build();
        store.appendEntries(List.of(noOp));

        long lastLogIndex = store.getLastLogIndex();
        for (String peerId : peerStubs.keySet()) {
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
            for (String peerId : peerStubs.keySet()) {
                replicateTo(peerId);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Steps down to follower, whether because we just learned of a higher
     * term, some other server has already established itself as leader for
     * our current term (§5.2), or we just finished committing a
     * configuration that removed us from the cluster (§6).
     */
    private void becomeFollowerLocked(long newTerm) {
        if (newTerm > store.getCurrentTerm()) {
            store.setTermAndVote(newTerm, null);
        }
        if (role == ServerRole.LEADER && heartbeatTask != null) {
            heartbeatTask.cancel(false);
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
                return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(false).build();
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

            // Rule 2: our log must actually have an entry at prevLogIndex
            // whose term matches, or we can't safely accept what follows it.
            // getTermAt also resolves correctly if prevLogIndex happens to
            // be exactly our snapshot boundary (§7), where the entry itself
            // no longer physically exists but its term is still known.
            if (request.getPrevLogIndex() > 0) {
                long prevTerm = store.getTermAt(request.getPrevLogIndex());
                if (prevTerm < 0 || prevTerm != request.getPrevLogTerm()) {
                    return AppendEntriesResponse.newBuilder().setTerm(currentTerm).setSuccess(false).build();
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
            if (request.getLeaderCommit() > commitIndex.get()) {
                commitIndex.set(Math.min(request.getLeaderCommit(), lastNewIndex));
                applyCommittedEntries();
            }

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
        RaftServiceGrpc.RaftServiceFutureStub stub = peerStubs.get(peerId);
        if (stub == null) {
            return; // removed from the configuration since this round started
        }
        long peerNextIndex = nextIndex.getOrDefault(peerId, 1L);
        long snapshotIndex = store.getSnapshotIndex();
        if (peerNextIndex <= snapshotIndex) {
            // This follower needs entries we've already compacted out of
            // our own log (§7) -- a follower that's fallen far behind, or a
            // brand-new member added via §6 that's never had anything
            // replicated to it. AppendEntries can't help here; send it our
            // snapshot instead.
            sendInstallSnapshot(peerId, stub);
            return;
        }
        long currentTerm = store.getCurrentTerm();
        long prevLogIndex = peerNextIndex - 1;
        // getTermAt (rather than getLogEntry(...).getTerm()) so this still
        // resolves correctly when prevLogIndex lands exactly on the
        // snapshot boundary, where the entry itself no longer physically
        // exists in the log.
        long prevLogTerm = Math.max(0, store.getTermAt(prevLogIndex));

        long lastLogIndex = store.getLastLogIndex();
        List<LogEntry> entries = new ArrayList<>();
        for (long i = peerNextIndex; i <= lastLogIndex; i++) {
            entries.add(store.getLogEntry(i));
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

        ListenableFuture<AppendEntriesResponse> future = stub.appendEntries(request);
        Futures.addCallback(future, new FutureCallback<AppendEntriesResponse>() {
            @Override
            public void onSuccess(AppendEntriesResponse response) {
                handleAppendEntriesResponse(peerId, currentTerm, lastSentIndex, response);
            }

            @Override
            public void onFailure(Throwable t) {
                // Peer unreachable; nextIndex stays put and we'll just try
                // again on the next heartbeat tick (§5.5).
            }
        }, MoreExecutors.directExecutor());
    }

    /**
     * Sends the next chunk of this server's snapshot to a follower that needs
     * entries we've already compacted away (§7, Figure 13). The snapshot is
     * split into fixed-size chunks; one chunk is sent per heartbeat cycle.
     * The transfer is tracked in {@link #snapshotTransfers} and progresses
     * as each chunk is acknowledged by the follower.
     */
    private void sendInstallSnapshot(String peerId, RaftServiceGrpc.RaftServiceFutureStub stub) {
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
        SnapshotTransfer transferRef = transfer;
        long newOffset = offset + len;

        ListenableFuture<InstallSnapshotResponse> future = stub.installSnapshot(request);
        Futures.addCallback(future, new FutureCallback<InstallSnapshotResponse>() {
            @Override
            public void onSuccess(InstallSnapshotResponse response) {
                handleSnapshotChunkResponse(peerId, currentTerm, transferRef, newOffset, done, response);
            }

            @Override
            public void onFailure(Throwable t) {
                lock.lock();
                try {
                    transferRef.inFlight = false;
                } finally {
                    lock.unlock();
                }
            }
        }, MoreExecutors.directExecutor());
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
            if (response.getTerm() > store.getCurrentTerm()) {
                becomeFollowerLocked(response.getTerm());
                return;
            }
            if (role != ServerRole.LEADER || store.getCurrentTerm() != sentTerm) {
                return; // no longer leading the term this request was sent under
            }
            if (response.getSuccess()) {
                matchIndex.put(peerId, lastSentIndex);
                nextIndex.put(peerId, lastSentIndex + 1);
                advanceCommitIndex();
            } else {
                // Log mismatch: back this follower's nextIndex up by one
                // and we'll offer it an earlier prevLogIndex next time (§5.3).
                long current = nextIndex.getOrDefault(peerId, 1L);
                nextIndex.put(peerId, Math.max(1, current - 1));
            }
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
        int neededForMajority = majority();
        for (long n = lastLogIndex; n > commitIndex.get(); n--) {
            LogEntry entry = store.getLogEntry(n);
            if (entry == null || entry.getTerm() != currentTerm) {
                continue;
            }
            int matches = 1; // the leader's own copy
            for (long m : matchIndex.values()) {
                if (m >= n) matches++;
            }
            if (matches >= neededForMajority) {
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
                boolean stillAMember = entry.getConfiguration().getMembersList().stream()
                        .anyMatch(member -> member.getId().equals(config.selfId()));
                if (role == ServerRole.LEADER && !stillAMember) {
                    // §6: we've now seen the configuration that removes us
                    // through to commitment -- our job here is done, so we
                    // step down and let a leader be (re-)elected from
                    // whoever is actually still a member.
                    log("stepping down: committed configuration no longer includes us");
                    becomeFollowerLocked(store.getCurrentTerm());
                }
            } else {
                result = stateMachine.apply(entry.getCommand().toByteArray());
            }
            CompletableFuture<byte[]> pending = pendingClientRequests.remove(index);
            if (pending != null) {
                pending.complete(result);
            }
        }
        maybeTakeSnapshotLocked();
    }

    // ------------------------------------------------------------------
    // Log compaction / snapshotting (§7). Every server does this
    // independently, not just the leader -- otherwise each one's log would
    // grow without bound regardless of who's currently leading. Synchronous
    // and runs while holding `lock`, the same as the rest of Raft's
    // decision-making here, so a slow takeSnapshot()/saveSnapshotAndCompact()
    // call briefly pauses replication and elections on this server; the
    // paper's own systems avoid that with copy-on-write or a forked child
    // process, which is out of scope for this implementation (see README).
    // ------------------------------------------------------------------

    /** Takes a new snapshot if enough newly applied entries have piled up since the last one. Caller must hold {@code lock}. */
    private void maybeTakeSnapshotLocked() {
        if (lastApplied.get() - store.getSnapshotIndex() < config.snapshotThreshold()) {
            return;
        }
        takeSnapshotLocked();
    }

    /** Unconditionally snapshots through {@code lastApplied}, if there's anything new to capture. Caller must hold {@code lock}. */
    private void takeSnapshotLocked() {
        long applied = lastApplied.get();
        if (applied <= store.getSnapshotIndex()) {
            return; // nothing new since the last snapshot
        }
        long includedTerm = store.getTermAt(applied);
        byte[] stateMachineData = stateMachine.takeSnapshot();
        // Captured here, not derived from the log, precisely so a §6
        // configuration entry that's about to be compacted away isn't lost:
        // currentConfiguration already reflects everything up to and
        // including `applied`, whether or not its source entry survives.
        byte[] configurationData = toProto(currentConfiguration).toByteArray();
        store.saveSnapshotAndCompact(new RaftStorage.Snapshot(applied, includedTerm, stateMachineData, configurationData));
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
            takeSnapshotLocked();
        } finally {
            lock.unlock();
        }
    }

    /** This server's current snapshot boundary (§7), or 0 if it has never taken or installed one. */
    public long snapshotIndex() {
        return store.getSnapshotIndex();
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
                return failedFuture(new NotLeaderException(currentLeaderId));
            }
            return appendAndReplicateLocked(LogEntry.newBuilder().setCommand(ByteString.copyFrom(command)));
        } finally {
            lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // Cluster reconfiguration (§6): membership changes one server at a
    // time. Changing only a single member per step guarantees the old and
    // new configurations' majorities always share at least one server, so
    // -- unlike an arbitrary multi-server change -- this can never produce
    // two disjoint majorities that elect two different leaders for the
    // same term. That's what lets this skip the general-case joint
    // consensus (C_old,new) machinery the paper introduces: a configuration
    // change here is just one more log entry, replicated and committed
    // exactly like a normal command. A second change has to wait for the
    // first to commit (enforced below) -- this simplification depends on
    // never having two membership changes in flight at once.
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
            log("proposing to add " + id + " (" + address + ")");
            return appendAndReplicateLocked(LogEntry.newBuilder().setConfiguration(toProto(updated)));
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
            return appendAndReplicateLocked(LogEntry.newBuilder().setConfiguration(toProto(updated)));
        } finally {
            lock.unlock();
        }
    }

    /** Returns a failed future if a previous configuration change hasn't committed yet, or null if it's fine to proceed. */
    private CompletableFuture<byte[]> rejectIfConfigurationChangePending() {
        if (currentConfigurationIndex > commitIndex.get()) {
            return failedFuture(new ConfigurationChangeException(
                    "a previous configuration change has not committed yet; retry once it has"));
        }
        return null;
    }

    /** Appends entryBuilder as the next log entry and replicates it to every current peer. Caller must hold {@code lock}. */
    private CompletableFuture<byte[]> appendAndReplicateLocked(LogEntry.Builder entryBuilder) {
        long index = store.getLastLogIndex() + 1;
        LogEntry entry = entryBuilder.setIndex(index).setTerm(store.getCurrentTerm()).build();
        store.appendEntries(List.of(entry));
        if (entry.hasConfiguration()) {
            // §6: the latest configuration in our log governs immediately,
            // even before it commits -- including which peers we replicate
            // this very entry to below.
            applyConfigurationLocked(entry.getIndex(), entry.getConfiguration());
        }

        CompletableFuture<byte[]> future = new CompletableFuture<>();
        pendingClientRequests.put(index, future);

        for (String peerId : peerStubs.keySet()) {
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

    public ServerRole role() { return role; }

    public String currentLeaderId() { return currentLeaderId; }

    /** This server's current view of the cluster's membership ("id" -> "host:port"), including itself. */
    public Map<String, String> currentConfiguration() {
        return currentConfiguration;
    }

    public static final class NotLeaderException extends RuntimeException {
        public final String leaderHint;

        public NotLeaderException(String leaderHint) {
            super("not the leader" + (leaderHint != null ? "; try " + leaderHint : ""));
            this.leaderHint = leaderHint;
        }
    }

    /** Thrown by {@link #addServer} / {@link #removeServer} for requests that are invalid regardless of who the leader is. */
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
        applyConfigurationLocked(0, toProto(config.peerAddresses()));
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
        this.currentConfiguration = Map.copyOf(members);
        this.currentConfigurationIndex = index;

        for (Map.Entry<String, String> member : members.entrySet()) {
            if (member.getKey().equals(config.selfId())) continue;
            peerStubs.computeIfAbsent(member.getKey(), id -> peerStubFactory.apply(member.getValue()));
        }

        List<String> noLongerMembers = new ArrayList<>();
        for (String peerId : peerStubs.keySet()) {
            if (!members.containsKey(peerId)) {
                noLongerMembers.add(peerId);
            }
        }
        for (String peerId : noLongerMembers) {
            closeStubChannel(peerStubs.remove(peerId));
            nextIndex.remove(peerId);
            matchIndex.remove(peerId);
            snapshotTransfers.remove(peerId);
        }
    }

    private static ClusterConfiguration toProto(Map<String, String> members) {
        ClusterConfiguration.Builder builder = ClusterConfiguration.newBuilder();
        for (Map.Entry<String, String> member : members.entrySet()) {
            builder.addMembers(ClusterConfiguration.Member.newBuilder()
                    .setId(member.getKey())
                    .setAddress(member.getValue())
                    .build());
        }
        return builder.build();
    }

    private static void closeStubChannel(RaftServiceGrpc.RaftServiceFutureStub stub) {
        if (stub != null && stub.getChannel() instanceof ManagedChannel managedChannel) {
            managedChannel.shutdownNow();
        }
    }

    /** Majority size for the *current* configuration -- recomputed every time, since §6 lets this change at runtime. */
    private int majority() {
        return currentConfiguration.size() / 2 + 1;
    }

    private void log(String msg) {
        System.out.println("[" + config.selfId() + "] " + msg);
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
