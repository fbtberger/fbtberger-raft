/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;

import javax.net.ssl.SSLException;
import java.io.File;
import java.util.Properties;

public final class TlsConfig {

    private final boolean enabled;
    private final File certFile;
    private final File keyFile;
    private final File caFile;
    private final boolean mtlsEnabled;

    public TlsConfig(boolean enabled, File certFile, File keyFile, File caFile, boolean mtlsEnabled) {
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
        if (!enabled) return disabled();

        File cert = new File(require(props, "tls.cert.path"));
        File key = new File(require(props, "tls.key.path"));
        File ca = new File(require(props, "tls.ca.path"));
        boolean mtls = Boolean.parseBoolean(props.getProperty("tls.mtls.enabled", "false"));
        return new TlsConfig(true, cert, key, ca, mtls);
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

    public SslContext buildServerSslContext() throws SSLException {
        SslContextBuilder builder = SslContextBuilder.forServer(certFile, keyFile)
                .trustManager(caFile);
        if (mtlsEnabled) {
            builder.clientAuth(ClientAuth.REQUIRE);
        }
        return builder.build();
    }

    public SslContext buildClientSslContext() throws SSLException {
        SslContextBuilder builder = SslContextBuilder.forClient()
                .keyManager(certFile, keyFile)
                .trustManager(caFile);
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
     * on OpenSSL 1.1.1+/3.x without {@code -traditional}.
     */
    public javax.net.ssl.SSLContext buildJdkSslContext() throws Exception {
        java.security.cert.CertificateFactory cf = java.security.cert.CertificateFactory.getInstance("X.509");
        java.security.cert.X509Certificate cert;
        try (java.io.InputStream in = new java.io.FileInputStream(certFile)) {
            cert = (java.security.cert.X509Certificate) cf.generateCertificate(in);
        }

        String keyPem = new String(java.nio.file.Files.readAllBytes(keyFile.toPath()), java.nio.charset.StandardCharsets.US_ASCII)
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");
        byte[] keyBytes = java.util.Base64.getDecoder().decode(keyPem);
        java.security.PrivateKey privateKey = java.security.KeyFactory.getInstance("RSA")
                .generatePrivate(new java.security.spec.PKCS8EncodedKeySpec(keyBytes));

        java.security.KeyStore keyStore = java.security.KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setKeyEntry("node", privateKey, new char[0], new java.security.cert.Certificate[]{cert});

        javax.net.ssl.KeyManagerFactory kmf =
                javax.net.ssl.KeyManagerFactory.getInstance(javax.net.ssl.KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, new char[0]);

        javax.net.ssl.SSLContext sslContext = javax.net.ssl.SSLContext.getInstance("TLS");
        sslContext.init(kmf.getKeyManagers(), null, null);
        return sslContext;
    }
}
