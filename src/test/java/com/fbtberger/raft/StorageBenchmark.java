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
import java.util.concurrent.CompletableFuture;
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
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
public class StorageBenchmark {

    // ── 1 & 2: appends — rebuilt in v114 ─────────────────────────────────────
    //
    // The first version of these was wrong, and wrong in a way worth recording, because the
    // numbers it produced looked like numbers:
    //
    //   * The store was created per ITERATION, but every invocation appended to it. Within a
    //     one-second iteration that is tens of thousands of appends, so the log grew by millions
    //     of entries while being measured. Later invocations paid for segment rollover and a
    //     larger B-tree than earlier ones. The benchmark measured a moving target.
    //
    //   * appendDeferringTheSync was fire-and-forget: it queued fsyncs faster than the disk could
    //     retire them, the backlog grew until it collapsed, and the error bars came out several
    //     times larger than the scores. It also compared two different promises — one variant
    //     returned when the data was durable, the other when it had merely been asked to be.
    //
    // The result was BDB appearing FOUR TIMES SLOWER with a deferred fsync than with a blocking
    // one. A measurement that violates physics is not measuring physics.
    //
    // The rebuild: a fresh store per invocation (set up, but not timed), a fixed amount of work,
    // and both variants must reach the SAME durability before the clock stops. Then the only thing
    // left between them is whether deferring lets the fsyncs pipeline — which is the actual
    // question §10.2.1 poses.

    /** How many batches each timed invocation appends. Fixed work, so growth cannot drift. */
    private static final int BATCHES_PER_INVOCATION = 200;

    @State(Scope.Thread)
    public static class AppendState {

        @Param({"wal", "bdb"})
        public String impl;

        /** Raft batches AppendEntries, and one batch pays for ONE fsync however large it is. */
        @Param({"1", "10", "100"})
        public int batchSize;

        /**
         * Entries already in the log before the clock starts. An append into an empty store and an
         * append into a store with a real log behind it are different operations — and it is the
         * second one that a running cluster actually performs.
         */
        @Param({"0", "10000"})
        public int prefill;

        Path dir;
        RaftStorage store;
        long nextIndex;
        List<LogEntry> batch;

        @Setup(Level.Invocation)
        public void freshStore() throws IOException {
            dir = Files.createTempDirectory("raft-bench-append-");
            store = open(impl, dir);
            batch = new ArrayList<>(batchSize);
            nextIndex = 1;
            if (prefill > 0) {
                List<LogEntry> chunk = new ArrayList<>(500);
                for (int written = 0; written < prefill; written += 500) {
                    chunk.clear();
                    for (int i = 0; i < Math.min(500, prefill - written); i++) {
                        chunk.add(LogEntry.newBuilder()
                                .setIndex(nextIndex++).setTerm(1).setCommand(PAYLOAD).build());
                    }
                    store.appendEntries(chunk);
                }
            }
        }

        @TearDown(Level.Invocation)
        public void discardStore() throws IOException {
            if (store != null) {
                store.close();
                store = null;
            }
            deleteRecursively(dir);
        }

        List<LogEntry> nextBatch() {
            batch.clear();
            for (int i = 0; i < batchSize; i++) {
                batch.add(LogEntry.newBuilder()
                        .setIndex(nextIndex++).setTerm(1).setCommand(PAYLOAD).build());
            }
            return batch;
        }
    }

    /** Blocking fsync per batch: durable when the call returns. */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long appendAndSync(AppendState s) {
        for (int i = 0; i < BATCHES_PER_INVOCATION; i++) {
            s.store.appendEntries(s.nextBatch());
        }
        return s.store.getLastLogIndex();
    }

    /**
     * §10.2.1: the same work, the same durability by the time we stop the clock — but the fsyncs
     * were allowed to overlap with the appends instead of serialising behind them.
     *
     * <p>The {@code join()} at the end is not a formality. Without it this measures how fast we can
     * make promises, not how fast the disk can keep them, and the backlog quietly turns the numbers
     * into noise. That is precisely what the first version of this benchmark did.
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long appendDeferringTheSync(AppendState s) {
        List<CompletableFuture<Void>> pending = new ArrayList<>(BATCHES_PER_INVOCATION);
        for (int i = 0; i < BATCHES_PER_INVOCATION; i++) {
            pending.add(s.store.appendEntriesDeferSync(s.nextBatch()));
        }
        CompletableFuture.allOf(pending.toArray(new CompletableFuture[0])).join();
        return s.store.getLastLogIndex();
    }

    /**
     * AppendEntries rule 3 — the follower catch-up path that took the cluster down in July, and
     * that a healthy cluster never walks. Worth knowing what it costs when it finally does.
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public long truncateAndReAppend(AppendState s) {
        for (int i = 0; i < BATCHES_PER_INVOCATION; i++) {
            s.store.appendEntries(s.nextBatch());
            long from = Math.max(1, s.store.getLastLogIndex() - s.batchSize + 1);
            s.store.truncateFrom(from);
            s.nextIndex = from;
            s.store.appendEntries(s.nextBatch());
        }
        return s.store.getLastLogIndex();
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
