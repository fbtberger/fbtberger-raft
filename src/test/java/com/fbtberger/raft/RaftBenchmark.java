package com.fbtberger.raft;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.ByteString;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JMH benchmarks for Raft hot paths. Run via:
 * {@code java -cp build/libs/raft-java-1.0.0-all.jar org.openjdk.jmh.Main com.fbtberger.raft.RaftBenchmark}
 * or directly: {@code RaftBenchmark.main(new String[0])}
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Thread)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class RaftBenchmark {

    private RaftNode node;
    private InMemoryStorage storage;
    private long nextIndex;
    private AppendEntriesRequest heartbeat;

    @Setup(Level.Trial)
    public void setup() throws Exception {
        storage = new InMemoryStorage();
        KeyValueStateMachine sm = new KeyValueStateMachine();

        File tmpDir = Files.createTempDirectory("raft-bench").toFile();
        java.util.Properties props = new java.util.Properties();
        props.setProperty("node.id", "bench");
        props.setProperty("node.port", "0");
        props.setProperty("data.dir", tmpDir.getAbsolutePath());
        props.setProperty("snapshot.threshold", "1000000");
        props.setProperty("peer.bench", "localhost:0");
        java.nio.file.Path cfgFile = Files.createTempFile("bench-", ".properties");
        try (var out = Files.newOutputStream(cfgFile)) { props.store(out, null); }
        RaftConfig config = RaftConfig.load(cfgFile);

        node = new RaftNode(config, storage, sm, addr -> null, RaftMetrics.noop());
        node.start();
        Thread.sleep(200);

        storage.setTermAndVote(1, "bench");
        nextIndex = 1;

        heartbeat = AppendEntriesRequest.newBuilder()
                .setTerm(1).setLeaderId("bench")
                .setPrevLogIndex(0).setPrevLogTerm(0)
                .setLeaderCommit(0).build();
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (node != null) node.shutdown();
    }

    @Benchmark
    public AppendEntriesResponse handleAppendEntriesHeartbeat() {
        return node.handleAppendEntries(heartbeat);
    }

    @Benchmark
    public void walAppendInMemory() {
        long idx = nextIndex++;
        LogEntry entry = LogEntry.newBuilder()
                .setIndex(idx).setTerm(1)
                .setCommand(ByteString.copyFromUtf8("SET k" + idx + " v" + idx))
                .build();
        storage.appendEntries(List.of(entry));
    }

    @Benchmark
    public void walAppendAndReadBack() {
        long idx = nextIndex++;
        LogEntry entry = LogEntry.newBuilder()
                .setIndex(idx).setTerm(1)
                .setCommand(ByteString.copyFromUtf8("SET k" + idx + " v" + idx))
                .build();
        storage.appendEntries(List.of(entry));
        storage.getLogEntry(idx);
    }

    public static void main(String[] args) throws Exception {
        new Runner(new OptionsBuilder()
                .include(RaftBenchmark.class.getSimpleName())
                .build()).run();
    }
}
