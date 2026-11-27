/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import com.fbtberger.raft.proto.AppendEntriesRequest;
import com.fbtberger.raft.proto.InstallSnapshotRequest;
import com.fbtberger.raft.proto.PreVoteRequest;
import com.fbtberger.raft.proto.RequestVoteRequest;
import com.fbtberger.raft.proto.ReadIndexRequest;
import com.fbtberger.raft.proto.ReadIndexResponse;
import com.fbtberger.raft.proto.TimeoutNowRequest;
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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLPeerUnverifiedException;

public final class NettyTransportServer implements RaftTransportServer {

    private static final Logger LOG = LoggerFactory.getLogger(NettyTransportServer.class);

    private final int port;
    private final RaftRpcHandler handler;
    private final SslContext sslContext;
    private final TlsConfig tlsConfig;
    private final NioEventLoopGroup bossGroup;
    private final NioEventLoopGroup workerGroup;
    private Channel serverChannel;

    public NettyTransportServer(int port, RaftRpcHandler handler) {
        this(port, handler, null);
    }

    public NettyTransportServer(int port, RaftRpcHandler handler, TlsConfig tlsConfig) {
        this.port = port;
        this.handler = handler;
        this.tlsConfig = tlsConfig != null ? tlsConfig : TlsConfig.disabled();
        try {
            this.sslContext = tlsConfig != null && tlsConfig.enabled()
                    ? tlsConfig.buildServerSslContext() : null;
        } catch (SSLException e) {
            throw new RuntimeException("failed to create server TLS context", e);
        }
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
                            if (sslContext != null) {
                                ch.pipeline().addLast(sslContext.newHandler(ch.alloc()));
                            }
                            ch.pipeline()
                                    .addLast(new LengthFieldBasedFrameDecoder(16 * 1024 * 1024, 0, 4, 0, 4))
                                    .addLast(new LengthFieldPrepender(4))
                                    .addLast(new RequestHandler());
                        }
                    })
                    .bind(port)
                    .sync()
                    .channel();
            tlsConfig.logStartup("Netty", boundPort(), tlsConfig.mtlsEnabled());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("bind interrupted", e);
        }
    }

    /** The port actually bound, which differs from the one asked for when that was 0. */
    int boundPort() {
        return ((java.net.InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    @Override
    public void close() {
        // Wait for the listening socket to be released. close() on a channel only
        // schedules the close, so a caller that closed this server and bound the same
        // port straight away -- a restart in the same process, or the next test -- could
        // lose the race and get "Address already in use" from a server that was "closed".
        if (serverChannel != null) serverChannel.close().syncUninterruptibly();
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    /** One instance per connection, so what it learns about the peer is that connection's. */
    private final class RequestHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private boolean refused;
        private boolean identityRead;
        private String authenticatedNodeId;

        /**
         * The Netty counterpart of {@code GrpcTransportServer.RaftServiceAdapter}'s guard, and the
         * same rule: under mTLS the sender id a message declares must be the CN of the certificate
         * the connection authenticated with. See {@link PeerIdentity} for why PKI membership alone
         * is not enough. Until this existed the Netty server required a client certificate and
         * then never looked at whose it was.
         *
         * <p>Returns true when the request may proceed. Otherwise the connection is closed: this
         * protocol has no error frame, and a peer that forged its sender is not owed a polite
         * answer on a connection it should not be using. The client's pending request fails on
         * the close. Logged at ERROR for the reason the gRPC guard is -- a server that drops peer
         * traffic silently looks like a network fault.
         */
        private boolean senderIsWhoItClaims(ChannelHandlerContext ctx, String rpc, String claimedId) {
            if (!tlsConfig.enabled() || !tlsConfig.mtlsEnabled()) return true;
            // Frames already decoded when the connection was refused still arrive here; closing
            // is not instantaneous. They are not served, whatever they claim.
            if (refused) return false;
            String authenticated = authenticatedNodeId(ctx);
            if (authenticated != null && authenticated.equals(claimedId)) return true;

            LOG.error("Raft RPC {} refused: the sender claims to be '{}', the certificate "
                    + "authenticates '{}'. Closing the connection from {}.",
                    rpc, claimedId, authenticated, ctx.channel().remoteAddress());
            refused = true;
            ctx.close();
            return false;
        }

        /**
         * Read once per connection, after the handshake: a frame can only arrive here through the
         * SslHandler, so by now the session is established. Null means no usable identity, which
         * the caller treats as a refusal, not as an exemption.
         */
        private String authenticatedNodeId(ChannelHandlerContext ctx) {
            if (!identityRead) {
                identityRead = true;
                SslHandler ssl = ctx.pipeline().get(SslHandler.class);
                if (ssl != null) {
                    try {
                        Certificate[] chain = ssl.engine().getSession().getPeerCertificates();
                        if (chain != null && chain.length > 0 && chain[0] instanceof X509Certificate leaf) {
                            authenticatedNodeId = PeerIdentity.fromCertificate(leaf);
                        }
                    } catch (SSLPeerUnverifiedException e) {
                        authenticatedNodeId = null;
                    }
                }
            }
            return authenticatedNodeId;
        }

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
                        RequestVoteRequest req = RequestVoteRequest.parseFrom(payload);
                        if (!senderIsWhoItClaims(ctx, "RequestVote", req.getCandidateId())) return;
                        response = handler.handleRequestVote(req);
                        respType = NettyProtocol.REQUEST_VOTE_RESP;
                    }
                    case NettyProtocol.APPEND_ENTRIES_REQ -> {
                        AppendEntriesRequest req = AppendEntriesRequest.parseFrom(payload);
                        if (!senderIsWhoItClaims(ctx, "AppendEntries", req.getLeaderId())) return;
                        response = handler.handleAppendEntries(req);
                        respType = NettyProtocol.APPEND_ENTRIES_RESP;
                    }
                    case NettyProtocol.INSTALL_SNAPSHOT_REQ -> {
                        InstallSnapshotRequest req = InstallSnapshotRequest.parseFrom(payload);
                        if (!senderIsWhoItClaims(ctx, "InstallSnapshot", req.getLeaderId())) return;
                        response = handler.handleInstallSnapshot(req);
                        respType = NettyProtocol.INSTALL_SNAPSHOT_RESP;
                    }
                    case NettyProtocol.PRE_VOTE_REQ -> {
                        PreVoteRequest req = PreVoteRequest.parseFrom(payload);
                        if (!senderIsWhoItClaims(ctx, "PreVote", req.getCandidateId())) return;
                        response = handler.handlePreVote(req);
                        respType = NettyProtocol.PRE_VOTE_RESP;
                    }
                    case NettyProtocol.TIMEOUT_NOW_REQ -> {
                        TimeoutNowRequest req = TimeoutNowRequest.parseFrom(payload);
                        if (!senderIsWhoItClaims(ctx, "TimeoutNow", req.getLeaderId())) return;
                        response = handler.handleTimeoutNow(req);
                        respType = NettyProtocol.TIMEOUT_NOW_RESP;
                    }
                    case NettyProtocol.READ_INDEX_REQ -> {
                        ReadIndexRequest req = ReadIndexRequest.parseFrom(payload);
                        if (!senderIsWhoItClaims(ctx, "ReadIndex", req.getRequesterId())) return;
                        // Answered when the leader has confirmed itself, not on this event
                        // loop: blocking here for a heartbeat round would stall every other
                        // request on the connection. A failure is answered as "no", because
                        // on this wire an unanswered request id waits until the timeout.
                        handler.handleReadIndex(req).whenComplete((answer, failure) ->
                                write(ctx, NettyProtocol.READ_INDEX_RESP, reqId,
                                        failure == null ? answer : ReadIndexResponse.getDefaultInstance()));
                        return;
                    }
                    default -> throw new IllegalStateException("unknown request type: " + type);
                }
            } catch (InvalidProtocolBufferException e) {
                ctx.close();
                return;
            }

            write(ctx, respType, reqId, response);
        }

        private void write(ChannelHandlerContext ctx, byte respType, int reqId, MessageLite response) {
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
