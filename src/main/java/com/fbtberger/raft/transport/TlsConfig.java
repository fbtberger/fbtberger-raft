/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

public final class TlsConfig {

    private static final Logger LOG = LoggerFactory.getLogger(TlsConfig.class);

    /** How close to its expiry a node certificate has to be before startup says so. */
    static final Duration EXPIRY_WARNING = Duration.ofDays(30);

    private final boolean enabled;
    private final File certFile;
    private final File keyFile;
    private final File caFile;
    private final boolean mtlsEnabled;
    private final long reloadIntervalSeconds;
    private TlsMaterial material;

    /** Default for {@code tls.reload.interval.seconds}. */
    public static final long DEFAULT_RELOAD_INTERVAL_SECONDS = 30;

    public TlsConfig(boolean enabled, File certFile, File keyFile, File caFile, boolean mtlsEnabled) {
        this(enabled, certFile, keyFile, caFile, mtlsEnabled, DEFAULT_RELOAD_INTERVAL_SECONDS);
    }

    /**
     * @param reloadIntervalSeconds how often the three files are checked for a renewed
     *                              certificate; 0 = read once at start, as before
     */
    public TlsConfig(boolean enabled, File certFile, File keyFile, File caFile, boolean mtlsEnabled,
                     long reloadIntervalSeconds) {
        if (reloadIntervalSeconds < 0) {
            throw new IllegalArgumentException("tls.reload.interval.seconds must not be negative: "
                    + reloadIntervalSeconds);
        }
        this.reloadIntervalSeconds = reloadIntervalSeconds;
        this.enabled = enabled;
        this.certFile = certFile;
        this.keyFile = keyFile;
        this.caFile = caFile;
        this.mtlsEnabled = mtlsEnabled;
    }

    public static TlsConfig disabled() {
        return new TlsConfig(false, null, null, null, false);
    }

    public static TlsConfig fromProperties(Properties props) {
        boolean enabled = Boolean.parseBoolean(props.getProperty("tls.enabled", "false"));
        if (!enabled) {
            // tls.mtls.enabled is only read when tls.enabled is true. Someone who set the one
            // without the other asked for mutual authentication and gets plaintext, and until
            // now nothing said so: the node came up, the cluster worked, and the flag was dead.
            if (Boolean.parseBoolean(props.getProperty("tls.mtls.enabled", "false"))) {
                LOG.warn("tls.mtls.enabled=true is IGNORED because tls.enabled=false: "
                        + "the peer transport runs in plaintext, with no encryption and no "
                        + "client certificates. Set tls.enabled=true to get mTLS.");
            }
            return disabled();
        }

        File cert = new File(require(props, "tls.cert.path"));
        File key = new File(require(props, "tls.key.path"));
        File ca = new File(require(props, "tls.ca.path"));
        boolean mtls = Boolean.parseBoolean(props.getProperty("tls.mtls.enabled", "false"));
        long reload = Long.parseLong(props.getProperty("tls.reload.interval.seconds",
                Long.toString(DEFAULT_RELOAD_INTERVAL_SECONDS)));
        return new TlsConfig(true, cert, key, ca, mtls, reload);
    }

    public long reloadIntervalSeconds() { return reloadIntervalSeconds; }

    /**
     * The certificate, key and CA every TLS context of this node is built from, loaded on
     * first use and from then on re-read when the files change (see {@link TlsMaterial}).
     * One instance per configuration, so the peer transport, the client channel and the
     * metrics endpoint all switch to a renewed certificate together.
     */
    public synchronized TlsMaterial material() throws SSLException {
        if (!enabled) {
            throw new IllegalStateException("TLS is disabled: there is no material");
        }
        if (material == null) {
            try {
                material = new TlsMaterial(certFile, keyFile, caFile);
            } catch (java.io.IOException | java.security.GeneralSecurityException e) {
                throw new SSLException("could not load TLS material (cert=" + certFile + ", key=" + keyFile
                        + ", ca=" + caFile + "): " + e.getMessage(), e);
            }
            material.watch(reloadIntervalSeconds);
        }
        return material;
    }

    private static String require(Properties props, String key) {
        String v = props.getProperty(key);
        if (v == null) throw new IllegalArgumentException("tls.enabled=true but missing: " + key);
        return v;
    }

    public boolean enabled()     { return enabled; }
    public File certFile()       { return certFile; }
    public File keyFile()        { return keyFile; }
    public File caFile()         { return caFile; }
    public boolean mtlsEnabled() { return mtlsEnabled; }

    /**
     * Says, once, at startup, what security a peer transport server actually came up with.
     *
     * <p>Before this, a node logged nothing about TLS at all. Whether a cluster was running
     * plaintext, one-way TLS or mTLS could only be found out by reading its properties file or
     * by pointing {@code openssl s_client} at it, and a misconfiguration that silently fell
     * back to less (see {@link #fromProperties}) was invisible from the log. This line is the
     * answer to "what is this node actually doing", stated by the node itself.
     *
     * <p>For TLS it also names the node certificate: its CN, because that is the identity peers
     * bind the sender id to (see {@link PeerIdentity}), and its expiry, because an expired node
     * certificate takes the node out of the cluster with nothing but handshake failures on the
     * other side. Problems with the certificate are warned about, never thrown: building the SSL
     * context is what decides whether the node can start, and it already fails loudly.
     *
     * @param transport     the transport's name, e.g. {@code "gRPC"} or {@code "Netty"}
     * @param port          the port the server is bound to
     * @param bindsSenderId whether that server compares the declared sender id against the
     *                      certificate CN under mTLS
     */
    public void logStartup(String transport, int port, boolean bindsSenderId) {
        logStartup(transport, port, bindsSenderId, Instant.now());
    }

    void logStartup(String transport, int port, boolean bindsSenderId, Instant now) {
        if (!enabled) {
            LOG.warn("Raft peer transport ({}) on port {}: PLAINTEXT (tls.enabled=false). Peer "
                    + "traffic is neither encrypted nor authenticated.", transport, port);
            return;
        }
        if (mtlsEnabled) {
            LOG.info("Raft peer transport ({}) on port {}: mTLS, client certificates required, "
                            + "sender id {} the certificate CN",
                    transport, port, bindsSenderId ? "bound to" : "NOT bound to");
        } else {
            LOG.info("Raft peer transport ({}) on port {}: TLS, server-authenticated only "
                    + "(tls.mtls.enabled=false). Peers present no certificate and their "
                    + "sender ids are not checked.", transport, port);
        }

        X509Certificate cert;
        try (InputStream in = new FileInputStream(certFile)) {
            cert = (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(in);
        } catch (Exception e) {
            LOG.warn("Raft peer transport ({}): could not read the node certificate {}: {}",
                    transport, certFile, e.toString());
            return;
        }
        String cn = PeerIdentity.fromCertificate(cert);
        Instant notAfter = cert.getNotAfter().toInstant();
        LOG.info("Raft peer transport ({}): node certificate CN={}, issuer {}, valid until {} "
                        + "(cert={}, key={}, ca={})",
                transport, cn, cert.getIssuerX500Principal().getName(), notAfter,
                certFile, keyFile, caFile);

        if (!notAfter.isAfter(now)) {
            LOG.error("Raft peer transport ({}): the node certificate EXPIRED at {}. Peers will "
                    + "refuse every handshake with this node.", transport, notAfter);
        } else if (Duration.between(now, notAfter).compareTo(EXPIRY_WARNING) < 0) {
            LOG.warn("Raft peer transport ({}): the node certificate expires at {}, in less than "
                    + "{} days.", transport, notAfter, EXPIRY_WARNING.toDays());
        }
        if (mtlsEnabled && cn == null) {
            LOG.warn("Raft peer transport ({}): the node certificate has no CN. Peers that bind "
                    + "the sender id to the certificate will refuse this node's RPCs.", transport);
        }
    }

    /**
     * Explicitly {@link SslProvider#JDK} on both contexts below — found live on a real 3-node
     * cluster: without this, Netty (via {@code grpc-netty-shaded}, which bundles its own native
     * BoringSSL/OpenSSL engine) auto-selects that native engine over the JDK's own, and it
     * failed every single handshake with {@code SSLHandshakeException: General OpenSslEngine
     * problem} — a notoriously unhelpful, generic error from netty-tcnative's native PEM/key
     * parsing being stricter (or just different) than the JDK's own SunJSSE parser, for reasons
     * that proved impractical to pin down further at the native level. Forcing the JDK provider
     * sidesteps this entire class of native-engine issues; the JDK's own TLS implementation is
     * well-tested and portable, and fbtberger-raft's peer transport correctness matters far more
     * here than the native engine's modest throughput edge.
     */
    public SslContext buildServerSslContext() throws SSLException {
        SslContextBuilder builder = SslContextBuilder.forServer(material().keyManager())
                .sslProvider(SslProvider.JDK)
                .trustManager(material().trustManager());
        if (mtlsEnabled) {
            builder.clientAuth(ClientAuth.REQUIRE);
        }
        return builder.build();
    }

    public SslContext buildClientSslContext() throws SSLException {
        SslContextBuilder builder = SslContextBuilder.forClient()
                .sslProvider(SslProvider.JDK)
                .keyManager(material().keyManager())
                .trustManager(material().trustManager());
        return builder.build();
    }

    /**
     * Builds a JDK {@link javax.net.ssl.SSLContext} from the SAME PEM cert/key this class
     * already holds for the Netty peer transport — used by the metrics HTTPS server (Change 78,
     * {@code com.sun.net.httpserver.HttpsServer}), which needs a JDK SSLContext, not Netty's own
     * {@link SslContext} type. Reusing the same node identity avoids needing a second,
     * separately-managed keystore just for the metrics endpoint.
     *
     * <p>Expects the private key in PKCS#8 PEM form ({@code BEGIN PRIVATE KEY}, not
     * {@code BEGIN RSA PRIVATE KEY}) — the default output of {@code openssl req -newkey rsa}
     * on OpenSSL 1.1.1+/3.x without {@code -traditional}. Built on {@link #material()}, so
     * the metrics endpoint follows a renewed certificate like the peer transport does.
     */
    public javax.net.ssl.SSLContext buildJdkSslContext() throws Exception {
        javax.net.ssl.SSLContext sslContext = javax.net.ssl.SSLContext.getInstance("TLS");
        sslContext.init(new javax.net.ssl.KeyManager[] {material().keyManager()}, null, null);
        return sslContext;
    }
}
