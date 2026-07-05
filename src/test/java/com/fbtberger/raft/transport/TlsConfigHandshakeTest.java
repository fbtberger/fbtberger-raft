/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Proves a real, socket-level TLS handshake succeeds using {@link TlsConfig#buildServerSslContext()}
 * / {@link TlsConfig#buildClientSslContext()} — not just that they construct without throwing.
 *
 * <p>This distinction matters: the real bug found live on a 3-node cluster
 * ({@code SSLHandshakeException: General OpenSslEngine problem}) only ever manifested during
 * the actual handshake, never at context-construction time — a test that only checked
 * {@code buildServerSslContext()}/{@code buildClientSslContext()} didn't throw would have
 * passed even with the bug present. Only a genuine handshake, over a real socket (not an
 * in-memory {@code EmbeddedChannel}, which doesn't exercise the native engine the same way),
 * actually proves the fix.
 */
class TlsConfigHandshakeTest {

    private EventLoopGroup serverGroup;
    private EventLoopGroup clientGroup;
    private Channel serverChannel;

    @AfterEach
    void tearDown() {
        if (serverChannel != null) {
            serverChannel.close();
        }
        if (serverGroup != null) {
            serverGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
        if (clientGroup != null) {
            clientGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
    }

    private static boolean opensslAvailable() {
        try {
            new ProcessBuilder("openssl", "version").start().waitFor();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static void runOpenssl(String... args) throws Exception {
        java.util.List<String> cmd = new java.util.ArrayList<>();
        cmd.add("openssl");
        cmd.addAll(java.util.Arrays.asList(args));
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        if (p.waitFor() != 0) {
            throw new IllegalStateException("openssl failed: " + out);
        }
    }

    @Test
    @DisplayName("a real client-server TLS handshake succeeds with the JDK provider — "
            + "the actual regression this test exists for")
    void realHandshakeSucceeds() throws Exception {
        assumeTrue(opensslAvailable(), "openssl not on PATH — skipping");

        Path dir = Files.createTempDirectory("tls-config-handshake-test-");
        Path caKey = dir.resolve("ca.key");
        Path caCert = dir.resolve("ca.crt");
        Path key = dir.resolve("node.key");
        Path cert = dir.resolve("node.crt");
        Path csr = dir.resolve("node.csr");

        runOpenssl("req", "-x509", "-newkey", "rsa:2048", "-keyout", caKey.toString(),
                "-out", caCert.toString(), "-days", "1", "-nodes", "-subj", "/CN=test-ca");
        runOpenssl("req", "-newkey", "rsa:2048", "-keyout", key.toString(), "-out", csr.toString(),
                "-nodes", "-subj", "/CN=localhost");
        runOpenssl("x509", "-req", "-in", csr.toString(), "-CA", caCert.toString(),
                "-CAkey", caKey.toString(), "-CAcreateserial", "-out", cert.toString(), "-days", "1");

        TlsConfig serverTls = new TlsConfig(true, cert.toFile(), key.toFile(), caCert.toFile(), false);
        TlsConfig clientTls = new TlsConfig(true, cert.toFile(), key.toFile(), caCert.toFile(), false);

        serverGroup = new NioEventLoopGroup(1);
        clientGroup = new NioEventLoopGroup(1);

        serverChannel = new ServerBootstrap()
                .group(serverGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(serverTls.buildServerSslContext().newHandler(ch.alloc()));
                    }
                })
                .bind(0)
                .sync()
                .channel();

        int port = ((InetSocketAddress) serverChannel.localAddress()).getPort();

        Channel clientChannel = new Bootstrap()
                .group(clientGroup)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline().addLast(clientTls.buildClientSslContext().newHandler(ch.alloc()));
                    }
                })
                .connect("localhost", port)
                .sync()
                .channel();

        try {
            SslHandler sslHandler = clientChannel.pipeline().get(SslHandler.class);
            // The actual regression check: this future used to fail with
            // "SSLHandshakeException: General OpenSslEngine problem" every single time before
            // forcing SslProvider.JDK. get() throws if the handshake failed.
            assertDoesNotThrow(() -> sslHandler.handshakeFuture().get(10, TimeUnit.SECONDS));
            assertTrue(sslHandler.handshakeFuture().isSuccess(), "handshake did not report success");
        } finally {
            clientChannel.close();
        }
    }
}
