/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.AppendEntriesResponse;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.InstallSnapshotResponse;
import com.fbtberger.raft.proto.PreVoteRequest;
import com.fbtberger.raft.proto.PreVoteResponse;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.RequestVoteResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
import com.fbtberger.raft.proto.TimeoutNowResponse;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client side of the Netty peer transport: one connection, requests correlated to responses by
 * a request id.
 *
 * <h2>Every future completes</h2>
 * A request's future used to be completed by exactly two things: its response arriving, or an
 * exception reaching the pipeline. Neither happens when the peer simply closes the connection,
 * and that is what a peer does in the cases that matter here -- it rejects the client
 * certificate, it fails to parse a frame, its RPC handler throws, or the process dies. The
 * futures of every request in flight then stayed incomplete forever: without
 * {@link TimeoutTransport} in front the caller hung, with it the caller waited a full RPC timeout
 * for an answer that was known to be impossible, and the entries stayed in {@link #pending} for
 * the life of the object. Requests sent after the close could also be registered after the
 * sweep that was meant to fail them.
 *
 * <p>So the rule now is: once the channel is closed, for whatever reason, every pending future is
 * failed, and every later request fails immediately. A response that arrives but cannot be
 * decoded fails its own future instead of leaving it removed from the map and never completed.
 */
public final class NettyTransport implements RaftTransport {

    private final Channel channel;
    private final NioEventLoopGroup group;
    private final String peer;
    private final AtomicInteger nextReqId = new AtomicInteger(0);
    private final Map<Integer, CompletableFuture<?>> pending = new ConcurrentHashMap<>();
    /** Set exactly once, when the channel is known to be gone; every request after it fails. */
    private volatile Throwable closedCause;

    public NettyTransport(String host, int port, NioEventLoopGroup group, SslContext sslContext) {
        this.group = group;
        this.peer = host + ":" + port;
        try {
            this.channel = new Bootstrap()
                    .group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            if (sslContext != null) {
                                ch.pipeline().addLast(sslContext.newHandler(ch.alloc(), host, port));
                            }
                            ch.pipeline()
                                    .addLast(new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4))
                                    .addLast(new LengthFieldPrepender(4))
                                    .addLast(new ResponseHandler());
                        }
                    })
                    .connect(host, port)
                    .sync()
                    .channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("connect interrupted", e);
        }
        // The close future fires however the channel ends -- peer close, TLS rejection, local
        // close(), an exception -- and also if it closed before this line ran.
        channel.closeFuture().addListener(f ->
                failAll(new IOException("connection to peer " + peer + " closed")));
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
        return send(NettyProtocol.REQUEST_VOTE_REQ, request);
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
        return send(NettyProtocol.APPEND_ENTRIES_REQ, request);
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(InstallSnapshotRequest request) {
        return send(NettyProtocol.INSTALL_SNAPSHOT_REQ, request);
    }

    @Override
    public CompletableFuture<PreVoteResponse> preVote(PreVoteRequest request) {
        return send(NettyProtocol.PRE_VOTE_REQ, request);
    }

    @Override
    public CompletableFuture<TimeoutNowResponse> timeoutNow(TimeoutNowRequest request) {
        return send(NettyProtocol.TIMEOUT_NOW_REQ, request);
    }

    @Override
    public void close() {
        failAll(new IOException("transport to peer " + peer + " closed"));
        channel.close();
    }

    /** Number of requests still waiting for an answer. Visible so tests can show none leak. */
    int pendingCount() {
        return pending.size();
    }

    /**
     * Fails every request in flight, and every request after this, with {@code cause}. Only the
     * first cause is kept: it is the one that says why the connection went away.
     */
    private void failAll(Throwable cause) {
        if (closedCause == null) {
            closedCause = cause;
        }
        Throwable reason = closedCause;
        for (Integer reqId : pending.keySet()) {
            CompletableFuture<?> f = pending.remove(reqId);
            if (f != null) {
                f.completeExceptionally(reason);
            }
        }
    }

    private <T> CompletableFuture<T> send(byte type, MessageLite message) {
        CompletableFuture<T> future = new CompletableFuture<>();
        Throwable closed = closedCause;
        if (closed != null) {
            future.completeExceptionally(closed);
            return future;
        }
        int reqId = nextReqId.incrementAndGet();
        pending.put(reqId, future);
        // failAll may have swept the map between the check above and the put. If so this entry
        // was missed and nothing else will ever complete it.
        closed = closedCause;
        if (closed != null) {
            if (pending.remove(reqId) != null) {
                future.completeExceptionally(closed);
            }
            return future;
        }

        byte[] payload = message.toByteArray();
        ByteBuf buf = channel.alloc().buffer(NettyProtocol.HEADER_BYTES + payload.length);
        buf.writeByte(type);
        buf.writeInt(reqId);
        buf.writeBytes(payload);
        channel.writeAndFlush(buf).addListener(f -> {
            if (!f.isSuccess() && pending.remove(reqId) != null) {
                future.completeExceptionally(f.cause());
            }
        });
        return future;
    }

    private final class ResponseHandler extends SimpleChannelInboundHandler<ByteBuf> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            byte type = msg.readByte();
            int reqId = msg.readInt();
            byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);

            @SuppressWarnings("unchecked")
            CompletableFuture<Object> future = (CompletableFuture<Object>) pending.remove(reqId);
            if (future == null) return;

            // Anything that stops the decode -- a malformed payload, a response type this side
            // does not know -- has to land on this future: it is already out of the map, so
            // nothing else can reach it.
            try {
                future.complete(parseResponse(type, payload));
            } catch (InvalidProtocolBufferException | RuntimeException e) {
                future.completeExceptionally(e);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            failAll(cause);
            ctx.close();
        }

        private Object parseResponse(byte type, byte[] payload) throws InvalidProtocolBufferException {
            return switch (type) {
                case NettyProtocol.REQUEST_VOTE_RESP -> RequestVoteResponse.parseFrom(payload);
                case NettyProtocol.APPEND_ENTRIES_RESP -> AppendEntriesResponse.parseFrom(payload);
                case NettyProtocol.INSTALL_SNAPSHOT_RESP -> InstallSnapshotResponse.parseFrom(payload);
                case NettyProtocol.PRE_VOTE_RESP -> PreVoteResponse.parseFrom(payload);
                case NettyProtocol.TIMEOUT_NOW_RESP -> TimeoutNowResponse.parseFrom(payload);
                default -> throw new IllegalStateException("unknown response type: " + type);
            };
        }
    }
}
