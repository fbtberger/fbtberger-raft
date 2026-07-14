/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.ByteString;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * What the storage layer actually costs.
 *
 * <p>Benchmarks are only worth having if they answer a question. These answer three, and each one
 * is a decision this project has already made — so far on intuition.
 *
 * <h2>1. Is the deferred fsync (§10.2.1) worth its complexity?</h2>
 * {@code appendEntriesDeferSync} exists so that the append returns to the caller — who is holding
 * the Raft lock — <b>before</b> the fsync completes; replication and the disk sync then proceed in
 * parallel, and {@code leaderDiskMatchIndex} catches up afterwards. That is real machinery, and it
 * is only justified if the fsync is genuinely the expensive part. {@link #appendAndSync} versus
 * {@link #appendDeferringTheSync} is that number.
 *
 * <p>Read them as <b>latency on the critical path</b>, not as sustainable throughput: the deferred
 * variant does not wait for the disk, so its cost is what the Raft lock actually pays. The fsync
 * still happens — it just happens somewhere the leader is not blocked on it.
 *
 * <h2>2. How long does a restart take, and how does that grow?</h2>
 * <b>This is the one that matters operationally.</b> kwatro runs with snapshots turned OFF — log
 * replay <em>is</em> its persistence. That works, and it has the pleasant property that old
 * state-machine bugs heal on redeploy. It also means the log grows without bound, and
 * {@link #recoverFromAnExistingLog} is the cost of every single deploy, forever, as a function of
 * how long the cluster has been running.
 *
 * <p>The point of measuring it is to know <em>when</em> that stops being acceptable — i.e. when
 * snapshots stop being optional. A number beats a hunch, and the answer is not obvious: the curve
 * only bites once it does.
 *
 * <h2>3. WAL or Berkeley DB?</h2>
 * kwatro's data nodes run {@link BerkeleyDbStorage}. {@link WalStorage} is a segmented
 * write-ahead log with, in principle, much less to do per append. Every benchmark here is
 * parameterised over both, so the choice can be made on evidence.
 *
 * <h2>Running them</h2>
 * <pre>  ./gradlew jmh                                   # everything
 *  ./gradlew jmh -Pjmh.args="recoverFromAnExistingLog"   # one benchmark
 *  ./gradlew jmh -Pjmh.args="-p impl=wal -p batchSize=10"</pre>
 *
 * <p>These are not run by {@code ./gradlew build}: they take minutes, they hammer the disk, and a
 * benchmark that fails a CI build for being 8% slower on a loaded machine teaches people to ignore
 * CI. They are a measuring instrument, not a gate.
 */
@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class StorageBenchmark {

    /** Every implementation, so the comparison is on evidence rather than on which one we wrote first. */
    @Param({"wal", "bdb", "memory"})
    public String impl;

    /**
     * Entries per append. Raft batches AppendEntries, and a batch pays for <b>one</b> fsync no
     * matter how large it is — so this parameter is really asking how much of the cost is the
     * disk barrier and how much is the data.
     */
    @Param({"1", "10", "100"})
    public int batchSize;

    private Path dir;
    private RaftStorage store;
    private List<LogEntry> batch;
    private long nextIndex = 1;

    @Setup(Level.Iteration)
    public void openStore() throws IOException {
        dir = Files.createTempDirectory("raft-bench-");
        store = open(impl, dir);
        nextIndex = 1;
        batch = new ArrayList<>(batchSize);
    }

    @TearDown(Level.Iteration)
    public void closeStore() throws IOException {
        if (store != null) store.close();
        deleteRecursively(dir);
    }

    // ── 1. what the fsync costs ──────────────────────────────────────────────

    /** The full cost: the entries are on disk and synced when this returns. */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public long appendAndSync() {
        store.appendEntries(nextBatch());
        return store.getLastLogIndex();
    }

    /**
     * §10.2.1: what the Raft lock actually pays. The fsync is still coming — it is simply not on
     * this path. The gap between this and {@link #appendAndSync} IS the justification for the
     * deferred-sync machinery; if the gap is small, the machinery is not paying for itself.
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public long appendDeferringTheSync() {
        store.appendEntriesDeferSync(nextBatch());
        return store.getLastLogIndex();
    }

    // ── 2. the follower catch-up path ────────────────────────────────────────

    /**
     * AppendEntries rule 3 — the path that took the cluster down in July, and the one a healthy
     * cluster never walks. Worth knowing what it costs when it finally does: a follower that has
     * to discard a conflicting suffix and re-take the leader's.
     */
    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public long truncateAndReAppend() {
        store.appendEntries(nextBatch());
        long from = Math.max(1, store.getLastLogIndex() - batchSize + 1);
        store.truncateFrom(from);
        nextIndex = from;
        store.appendEntries(nextBatch());
        return store.getLastLogIndex();
    }

    private List<LogEntry> nextBatch() {
        batch.clear();
        for (int i = 0; i < batchSize; i++) {
            batch.add(LogEntry.newBuilder()
                    .setIndex(nextIndex++)
                    .setTerm(1)
                    .setCommand(PAYLOAD)
                    .build());
        }
        return batch;
    }

    // ── 3. the cost of every deploy, forever ─────────────────────────────────

    /**
     * Startup with snapshots off, which is how kwatro runs: the entire log is replayed. This is not
     * a micro-benchmark — it is the deploy.
     *
     * <p>Its own {@code @State}, because the log has to be built once and then re-opened over and
     * over; sharing the append state above would rebuild it every iteration and measure the wrong
     * thing entirely.
     */
    @State(Scope.Thread)
    @Fork(1)
    @Warmup(iterations = 1)
    @Measurement(iterations = 5)
    public static class RecoveryState {

        @Param({"wal", "bdb"})
        public String impl;

        /** How many entries the cluster has accumulated. The question is the SHAPE of this curve. */
        @Param({"1000", "10000", "50000"})
        public int logSize;

        Path dir;
        RaftStorage store;

        @Setup(Level.Trial)
        public void buildTheLog() throws IOException {
            dir = Files.createTempDirectory("raft-bench-recovery-");
            RaftStorage writer = open(impl, dir);
            List<LogEntry> chunk = new ArrayList<>(500);
            long index = 1;
            for (int written = 0; written < logSize; written += 500) {
                chunk.clear();
                for (int i = 0; i < Math.min(500, logSize - written); i++) {
                    chunk.add(LogEntry.newBuilder()
                            .setIndex(index++).setTerm(1).setCommand(PAYLOAD).build());
                }
                writer.appendEntries(chunk);
            }
            writer.close();
        }

        @TearDown(Level.Invocation)
        public void closeTheRecoveredStore() {
            if (store != null) {
                store.close();
                store = null;
            }
        }

        @TearDown(Level.Trial)
        public void removeTheLog() throws IOException {
            deleteRecursively(dir);
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long recoverFromAnExistingLog(RecoveryState state) {
        state.store = open(state.impl, state.dir);
        return state.store.getLastLogIndex();
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    /** ~120 bytes: a kwatro command is a small protobuf, not a blob. */
    private static final ByteString PAYLOAD =
            ByteString.copyFromUtf8("x".repeat(120));

    static RaftStorage open(String impl, Path dir) {
        return switch (impl) {
            case "wal"    -> new WalStorage(new File(dir.toFile(), "wal"));
            case "bdb"    -> new BerkeleyDbStorage(new File(dir.toFile(), "bdb"));
            case "memory" -> new InMemoryStorage();   // the floor: no disk at all
            default       -> throw new IllegalArgumentException("unknown storage: " + impl);
        };
    }

    static void deleteRecursively(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) return;
        try (var paths = Files.walk(dir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException ignored) {
                    // a benchmark leaving a temp file behind is not worth failing over
                }
            });
        }
    }
}
