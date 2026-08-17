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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class NettyTransport implements RaftTransport {

    private final Channel channel;
    private final NioEventLoopGroup group;
    private final AtomicInteger nextReqId = new AtomicInteger(0);
    private final Map<Integer, CompletableFuture<?>> pending = new ConcurrentHashMap<>();

    public NettyTransport(String host, int port, NioEventLoopGroup group, SslContext sslContext) {
        this.group = group;
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
        channel.close();
        pending.values().forEach(f -> f.completeExceptionally(new RuntimeException("transport closed")));
        pending.clear();
    }

    @SuppressWarnings("unchecked")
    private <T> CompletableFuture<T> send(byte type, MessageLite message) {
        int reqId = nextReqId.incrementAndGet();
        CompletableFuture<T> future = new CompletableFuture<>();
        pending.put(reqId, future);

        byte[] payload = message.toByteArray();
        ByteBuf buf = channel.alloc().buffer(NettyProtocol.HEADER_BYTES + payload.length);
        buf.writeByte(type);
        buf.writeInt(reqId);
        buf.writeBytes(payload);
        channel.writeAndFlush(buf).addListener(f -> {
            if (!f.isSuccess()) {
                pending.remove(reqId);
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

            try {
                future.complete(parseResponse(type, payload));
            } catch (InvalidProtocolBufferException e) {
                future.completeExceptionally(e);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
            pending.values().forEach(f -> f.completeExceptionally(cause));
            pending.clear();
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
