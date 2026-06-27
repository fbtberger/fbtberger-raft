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
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

import java.io.IOException;

public final class NettyTransportServer implements RaftTransportServer {

    private final int port;
    private final RaftRpcHandler handler;
    private final NioEventLoopGroup bossGroup;
    private final NioEventLoopGroup workerGroup;
    private Channel serverChannel;

    public NettyTransportServer(int port, RaftRpcHandler handler) {
        this.port = port;
        this.handler = handler;
        this.bossGroup = new NioEventLoopGroup(1, r -> {
            Thread t = new Thread(r, "raft-netty-boss");
            t.setDaemon(true);
            return t;
        });
        this.workerGroup = new NioEventLoopGroup(2, r -> {
            Thread t = new Thread(r, "raft-netty-worker");
            t.setDaemon(true);
            return t;
        });
    }

    @Override
    public void start() throws IOException {
        try {
            serverChannel = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4))
                                    .addLast(new LengthFieldPrepender(4))
                                    .addLast(new RequestHandler());
                        }
                    })
                    .bind(port)
                    .sync()
                    .channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("bind interrupted", e);
        }
    }

    @Override
    public void close() {
        if (serverChannel != null) serverChannel.close();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    private final class RequestHandler extends SimpleChannelInboundHandler<ByteBuf> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            byte type = msg.readByte();
            int reqId = msg.readInt();
            byte[] payload = new byte[msg.readableBytes()];
            msg.readBytes(payload);

            MessageLite response;
            byte respType;
            try {
                switch (type) {
                    case NettyProtocol.REQUEST_VOTE_REQ -> {
                        response = handler.handleRequestVote(RequestVoteRequest.parseFrom(payload));
                        respType = NettyProtocol.REQUEST_VOTE_RESP;
                    }
                    case NettyProtocol.APPEND_ENTRIES_REQ -> {
                        response = handler.handleAppendEntries(AppendEntriesRequest.parseFrom(payload));
                        respType = NettyProtocol.APPEND_ENTRIES_RESP;
                    }
                    case NettyProtocol.INSTALL_SNAPSHOT_REQ -> {
                        response = handler.handleInstallSnapshot(InstallSnapshotRequest.parseFrom(payload));
                        respType = NettyProtocol.INSTALL_SNAPSHOT_RESP;
                    }
                    case NettyProtocol.PRE_VOTE_REQ -> {
                        response = handler.handlePreVote(PreVoteRequest.parseFrom(payload));
                        respType = NettyProtocol.PRE_VOTE_RESP;
                    }
                    case NettyProtocol.TIMEOUT_NOW_REQ -> {
                        response = handler.handleTimeoutNow(TimeoutNowRequest.parseFrom(payload));
                        respType = NettyProtocol.TIMEOUT_NOW_RESP;
                    }
                    default -> throw new IllegalStateException("unknown request type: " + type);
                }
            } catch (InvalidProtocolBufferException e) {
                ctx.close();
                return;
            }

            byte[] respPayload = response.toByteArray();
            ByteBuf buf = ctx.alloc().buffer(NettyProtocol.HEADER_BYTES + respPayload.length);
            buf.writeByte(respType);
            buf.writeInt(reqId);
            buf.writeBytes(respPayload);
            ctx.writeAndFlush(buf);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
}
