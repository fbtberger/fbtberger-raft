package com.fbtberger.raft.transport;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.PreVoteRequest;
import com.fbtberger.raft.proto.PreVoteResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import com.google.protobuf.MessageLite;
import org.apache.hadoop.io.BytesWritable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public final class HadoopTransport implements RaftTransport {

    private final HadoopRaftProtocol proxy;

    HadoopTransport(HadoopRaftProtocol proxy) {
        this.proxy = proxy;
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return call(() -> RequestVoteResponse.parseFrom(
                unwrap(proxy.requestVote(wrap(request)))));
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
        return call(() -> AppendEntriesResponse.parseFrom(
                unwrap(proxy.appendEntries(wrap(request)))));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        return call(() -> InstallSnapshotResponse.parseFrom(
                unwrap(proxy.installSnapshot(wrap(request)))));
    }

    @Override
    public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest request) {
        return call(() -> PreVoteResponse.parseFrom(
                unwrap(proxy.preVote(wrap(request)))));
    }

    @Override
    public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest request) {
        return call(() -> TimeoutNowResponse.parseFrom(
                unwrap(proxy.timeoutNow(wrap(request)))));
    }

    @Override
    public void close() {
        org.apache.hadoop.ipc.RPC.stopProxy(proxy);
    }

    private static BytesWritable wrap(MessageLite msg) {
        byte[] bytes = msg.toByteArray();
        return new BytesWritable(bytes);
    }

    private static byte[] unwrap(BytesWritable bw) {
        byte[] raw = bw.getBytes();
        int len = bw.getLength();
        if (raw.length == len) return raw;
        byte[] trimmed = new byte[len];
        System.arraycopy(raw, 0, trimmed, 0, len);
        return trimmed;
    }

    private static <T> CompletableFuture<T> call(RpcCall<T> rpc) {
        CompletableFuture<T> future = new CompletableFuture<>();
        try {
            future.complete(rpc.call());
        } catch (IOException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @FunctionalInterface
    private interface RpcCall<T> {
        T call() throws IOException;
    }
}
