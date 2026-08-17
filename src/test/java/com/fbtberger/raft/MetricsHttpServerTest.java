/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves the fix for a real bug (found via an actual multi-node Docker deployment, not
 * discovered by this test suite beforehand): {@code RaftNodeConfiguration.metricsHttpServer()}
 * used to return the raw JDK {@code com.sun.net.httpserver.HttpServer} with
 * {@code @Bean(destroyMethod = "stop")} — but that class's only {@code stop} method takes an
 * {@code int} delay parameter, which Spring's destroy-method mechanism cannot invoke, so any
 * context that actually set {@code metrics.port > 0} failed at startup with
 * "has a non-boolean parameter — not supported as destroy method". No existing test ever set a
 * real {@code metrics.port} (see {@link RaftNodeTestConfiguration}'s own test properties, which
 * omit it), so this had never been exercised until it broke a real deployment.
 *
 * <p>Calls {@link RaftNodeConfiguration#metricsHttpServer} directly (a plain method call, no
 * Spring context needed — {@code RaftNodeConfiguration} is just a Java class with
 * {@code @Bean}-annotated factory methods) with a real, positive {@code metrics.port}, which is
 * exactly the configuration that used to fail.
 */
class MetricsHttpServerTest {

    private static final int TEST_METRICS_PORT = 19292; // arbitrary free-ish test port
    private MetricsHttpServer server;

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(); // the actual bug: this used to be impossible for Spring to call
        }
    }

    private static RaftConfig configWithMetricsPort(int metricsPort) throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/metrics-http-server-test-unused");
        props.setProperty("peer.n1", "localhost:9091");
        if (metricsPort > 0) {
            props.setProperty("metrics.port", String.valueOf(metricsPort));
        }

        Path tmp = Files.createTempFile("metrics-http-server-test-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    private static RaftNode singleNodeStarted(RaftConfig config) {
        RaftNode node = new RaftNode(config, new InMemoryStorage(), new KeyValueStateMachine(),
                address -> null, RaftMetrics.noop());
        node.start();
        return node;
    }

    @Test
    @DisplayName("metricsHttpServer() returns null when metrics.port is unset (unchanged guard behaviour)")
    void returnsNullWhenMetricsPortUnset() throws Exception {
        RaftConfig config = configWithMetricsPort(0);
        RaftNode node = singleNodeStarted(config);
        try {
            RaftNodeConfiguration cfg = new RaftNodeConfiguration();
            PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            HealthCheck healthCheck = new HealthCheck(node, new InMemoryStorage());

            server = cfg.metricsHttpServer(registry, config, healthCheck);

            assertNull(server, "metrics.port <= 0 must still produce no bean at all");
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("metricsHttpServer() with a real metrics.port returns a MetricsHttpServer whose "
            + "no-arg stop() Spring can actually call — this is the fix")
    void returnsWrapperWithCallableNoArgStop() throws Exception {
        RaftConfig config = configWithMetricsPort(TEST_METRICS_PORT);
        RaftNode node = singleNodeStarted(config);
        try {
            RaftNodeConfiguration cfg = new RaftNodeConfiguration();
            PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            HealthCheck healthCheck = new HealthCheck(node, new InMemoryStorage());

            server = cfg.metricsHttpServer(registry, config, healthCheck);

            assertNotNull(server, "metrics.port > 0 must produce a real server");
            // The actual regression check: a genuinely no-arg call, exactly what Spring's
            // destroyMethod = "stop" reflection will invoke. Before the fix this class didn't
            // exist — the bean itself was the raw HttpServer, whose stop(int) Spring couldn't
            // call at all, failing bean *definition validation*, not merely a runtime call.
            assertDoesNotThrow(() -> server.stop());
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("the wrapped server actually answers /health while running (not just a hollow wrapper)")
    void wrappedServerActuallyServesRequests() throws Exception {
        RaftConfig config = configWithMetricsPort(TEST_METRICS_PORT + 1);
        RaftNode node = singleNodeStarted(config);
        try {
            RaftNodeConfiguration cfg = new RaftNodeConfiguration();
            PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            HealthCheck healthCheck = new HealthCheck(node, new InMemoryStorage());

            server = cfg.metricsHttpServer(registry, config, healthCheck);

            HttpURLConnection conn = (HttpURLConnection)
                    URI.create("http://localhost:" + (TEST_METRICS_PORT + 1) + "/health").toURL().openConnection();
            conn.setConnectTimeout(2000);
            try {
                int status = conn.getResponseCode();
                assertTrue(status == 200 || status == 503,
                        "expected a real health JSON response (200 or 503), got " + status);
            } catch (IOException e) {
                fail("wrapped server did not answer /health at all: " + e.getMessage());
            } finally {
                conn.disconnect();
            }
        } finally {
            node.shutdown();
        }
    }

    @Test
    @DisplayName("with tls.enabled=true, metricsHttpServer() serves HTTPS (Change 78), not plaintext")
    void servesHttpsWhenTlsEnabled() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(opensslAvailable(), "openssl not on PATH — skipping");

        Path dir = Files.createTempDirectory("metrics-tls-test-");
        Path keyFile = dir.resolve("node.key");
        Path certFile = dir.resolve("node.crt");
        runOpenssl("req", "-x509", "-newkey", "rsa:2048", "-keyout", keyFile.toString(),
                "-out", certFile.toString(), "-days", "1", "-nodes", "-subj", "/CN=localhost");

        Properties props = new Properties();
        props.setProperty("node.id", "n1");
        props.setProperty("node.port", "9091");
        props.setProperty("data.dir", "/tmp/metrics-tls-test-unused");
        props.setProperty("peer.n1", "localhost:9091");
        int port = TEST_METRICS_PORT + 2;
        props.setProperty("metrics.port", String.valueOf(port));
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.cert.path", certFile.toString());
        props.setProperty("tls.key.path", keyFile.toString());
        props.setProperty("tls.ca.path", certFile.toString()); // self-signed: cert is its own CA

        Path propsFile = Files.createTempFile("metrics-tls-test-", ".properties");
        try (OutputStream out = Files.newOutputStream(propsFile)) {
            props.store(out, null);
        }
        RaftConfig config = RaftConfig.load(propsFile);
        RaftNode node = singleNodeStarted(config);
        try {
            RaftNodeConfiguration cfg = new RaftNodeConfiguration();
            PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            HealthCheck healthCheck = new HealthCheck(node, new InMemoryStorage());

            server = cfg.metricsHttpServer(registry, config, healthCheck);
            assertNotNull(server);

            javax.net.ssl.HttpsURLConnection conn = (javax.net.ssl.HttpsURLConnection)
                    URI.create("https://localhost:" + port + "/health").toURL().openConnection();
            // Test-only: trust-all, since this is a throwaway self-signed cert with no real CA
            // chain to validate against — proving the endpoint speaks TLS at all, not
            // validating a production trust chain (that's what the real deployment's shared
            // CA/truststore, generated by ops/tls/generate-certs.sh, is for).
            conn.setSSLSocketFactory(trustAllSslContext().getSocketFactory());
            conn.setHostnameVerifier((h, s) -> true);
            conn.setConnectTimeout(2000);
            try {
                int status = conn.getResponseCode();
                assertTrue(status == 200 || status == 503,
                        "expected a real HTTPS health response, got " + status);
            } finally {
                conn.disconnect();
            }
        } finally {
            node.shutdown();
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
        String out = new String(p.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
        int exit = p.waitFor();
        if (exit != 0) {
            fail("openssl failed (exit " + exit + "): " + out);
        }
    }

    private static javax.net.ssl.SSLContext trustAllSslContext() throws Exception {
        javax.net.ssl.TrustManager trustAll = new javax.net.ssl.X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() { return new java.security.cert.X509Certificate[0]; }
            public void checkClientTrusted(java.security.cert.X509Certificate[] c, String a) {}
            public void checkServerTrusted(java.security.cert.X509Certificate[] c, String a) {}
        };
        javax.net.ssl.SSLContext ctx = javax.net.ssl.SSLContext.getInstance("TLS");
        ctx.init(null, new javax.net.ssl.TrustManager[]{trustAll}, null);
        return ctx;
    }
}
