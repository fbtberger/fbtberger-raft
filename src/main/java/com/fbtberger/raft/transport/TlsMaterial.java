/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A node's certificate, key and CA, read from their files again whenever the files change.
 *
 * <h2>Why</h2>
 * Every TLS context in this project used to be built from the three PEM files once, at
 * start. A certificate that was renewed on disk therefore took effect at the next restart
 * and not before, and a certificate that expired first took the node out of the cluster
 * with nothing but handshake failures on the other side. With certificates that live for
 * weeks, renewing meant a rolling restart of every node, on a schedule, for ever.
 *
 * <h2>How</h2>
 * The contexts are now built from the key manager and the trust manager this class hands
 * out. Both are thin shells around the real ones, and a reload swaps what is inside. A
 * context is consulted at every handshake, so the next connection, in or out, uses the new
 * material. Connections that are already open keep the session they negotiated; nothing is
 * dropped.
 *
 * <h2>What a reload refuses</h2>
 * Renewal tools write the certificate and the key one after the other, and a poll can land
 * between the two. So a reload only takes effect if everything parses and the key belongs
 * to the certificate. Anything else is logged as an error and the material in use stays in
 * use: a half-written renewal must never take a working node off the network.
 */
public final class TlsMaterial {

    private static final Logger LOG = LoggerFactory.getLogger(TlsMaterial.class);

    /** One thread for every instance: a poll is three small file reads. */
    private static final ScheduledExecutorService POLLER = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "raft-tls-reload");
        t.setDaemon(true);
        return t;
    });

    private final File certFile;
    private final File keyFile;
    private final File caFile;

    private final ReloadingKeyManager keyManager = new ReloadingKeyManager();
    private final ReloadingTrustManager trustManager = new ReloadingTrustManager();
    private final AtomicLong reloads = new AtomicLong();
    private final AtomicLong failedReloads = new AtomicLong();

    private volatile byte[] fingerprint;
    private volatile X509Certificate certificate;

    /**
     * Loads the material once. Unlike a reload this throws: a node that cannot read its
     * certificate at start has nothing to fall back on.
     */
    public TlsMaterial(File certFile, File keyFile, File caFile) throws IOException, GeneralSecurityException {
        this.certFile = certFile;
        this.keyFile = keyFile;
        this.caFile = caFile;
        install(read());
    }

    /** For every TLS context of this node. The object stays the same across reloads. */
    public KeyManager keyManager() {
        return keyManager;
    }

    /** For every TLS context of this node. The object stays the same across reloads. */
    public TrustManager trustManager() {
        return trustManager;
    }

    /** The node certificate currently presented. */
    public X509Certificate certificate() {
        return certificate;
    }

    /** How often changed files were taken into use since start. */
    public long reloadCount() {
        return reloads.get();
    }

    /** How often changed files were found and refused. */
    public long failedReloadCount() {
        return failedReloads.get();
    }

    /**
     * Polls the files every {@code intervalSeconds}. The task holds this object weakly, so
     * a configuration that is no longer used stops being polled.
     */
    public void watch(long intervalSeconds) {
        if (intervalSeconds <= 0) {
            return;
        }
        WeakReference<TlsMaterial> self = new WeakReference<>(this);
        ScheduledFuture<?>[] task = new ScheduledFuture<?>[1];
        task[0] = POLLER.scheduleWithFixedDelay(() -> {
            TlsMaterial material = self.get();
            if (material == null) {
                task[0].cancel(false);
                return;
            }
            material.reloadIfChanged();
        }, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }

    /**
     * Takes the files into use if they differ from what is in use and are sound.
     *
     * @return true if new material is in use after this call
     */
    public synchronized boolean reloadIfChanged() {
        Loaded loaded;
        try {
            loaded = read();
        } catch (IOException | GeneralSecurityException | RuntimeException e) {
            // Only worth a line if the files changed: an unreadable file that was never
            // readable in this state would otherwise be reported at every poll.
            byte[] seen = fingerprintOrNull();
            if (seen == null || !Arrays.equals(seen, lastRefused)) {
                lastRefused = seen;
                failedReloads.incrementAndGet();
                LOG.error("TLS material changed on disk and was NOT taken into use; the previous "
                        + "certificate stays active (cert={}, key={}, ca={}): {}",
                        certFile, keyFile, caFile, e.toString());
            }
            return false;
        }
        if (Arrays.equals(loaded.fingerprint, fingerprint)) {
            return false;
        }
        X509Certificate previous = certificate;
        install(loaded);
        reloads.incrementAndGet();
        lastRefused = null;
        LOG.info("TLS material reloaded without restart: node certificate CN={}, valid until {} "
                        + "(was valid until {}). New connections use it from now on.",
                PeerIdentity.fromCertificate(loaded.chain[0]), loaded.chain[0].getNotAfter().toInstant(),
                previous == null ? "?" : previous.getNotAfter().toInstant());
        if (!loaded.chain[0].getNotAfter().toInstant().isAfter(Instant.now())) {
            LOG.error("The reloaded node certificate is already EXPIRED (valid until {}).",
                    loaded.chain[0].getNotAfter().toInstant());
        }
        return true;
    }

    private byte[] lastRefused;

    private void install(Loaded loaded) {
        keyManager.delegate = loaded.keyManager;
        trustManager.delegate = loaded.trustManager;
        certificate = loaded.chain[0];
        fingerprint = loaded.fingerprint;
    }

    private byte[] fingerprintOrNull() {
        try {
            return fingerprint(Files.readAllBytes(certFile.toPath()), Files.readAllBytes(keyFile.toPath()),
                    Files.readAllBytes(caFile.toPath()));
        } catch (IOException | GeneralSecurityException e) {
            return null;
        }
    }

    private static byte[] fingerprint(byte[] cert, byte[] key, byte[] ca) throws GeneralSecurityException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (byte[] part : new byte[][] {cert, key, ca}) {
            digest.update((byte) (part.length >>> 24));
            digest.update((byte) (part.length >>> 16));
            digest.update((byte) (part.length >>> 8));
            digest.update((byte) part.length);
            digest.update(part);
        }
        return digest.digest();
    }

    private Loaded read() throws IOException, GeneralSecurityException {
        byte[] certBytes = Files.readAllBytes(certFile.toPath());
        byte[] keyBytes = Files.readAllBytes(keyFile.toPath());
        byte[] caBytes = Files.readAllBytes(caFile.toPath());

        X509Certificate[] chain = certificates(certBytes);
        if (chain.length == 0) {
            throw new CertificateException("no certificate in " + certFile);
        }
        X509Certificate[] authorities = certificates(caBytes);
        if (authorities.length == 0) {
            throw new CertificateException("no certificate in " + caFile);
        }
        PrivateKey key = privateKey(keyBytes);
        requireKeyBelongsTo(key, chain[0]);

        KeyStore identity = KeyStore.getInstance("PKCS12");
        identity.load(null, null);
        identity.setKeyEntry("node", key, new char[0], chain);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(identity, new char[0]);

        KeyStore trusted = KeyStore.getInstance("PKCS12");
        trusted.load(null, null);
        for (int i = 0; i < authorities.length; i++) {
            trusted.setCertificateEntry("ca-" + i, authorities[i]);
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trusted);

        Loaded loaded = new Loaded();
        loaded.chain = chain;
        loaded.fingerprint = fingerprint(certBytes, keyBytes, caBytes);
        for (KeyManager km : kmf.getKeyManagers()) {
            if (km instanceof X509ExtendedKeyManager x509) {
                loaded.keyManager = x509;
            }
        }
        for (TrustManager tm : tmf.getTrustManagers()) {
            if (tm instanceof X509ExtendedTrustManager x509) {
                loaded.trustManager = x509;
            }
        }
        if (loaded.keyManager == null || loaded.trustManager == null) {
            throw new GeneralSecurityException("this JVM offers no X.509 key or trust manager");
        }
        return loaded;
    }

    private static X509Certificate[] certificates(byte[] pem) throws CertificateException {
        Collection<? extends Certificate> parsed =
                CertificateFactory.getInstance("X.509").generateCertificates(new ByteArrayInputStream(pem));
        return parsed.stream().map(c -> (X509Certificate) c).toArray(X509Certificate[]::new);
    }

    /** PKCS#8 PEM ({@code BEGIN PRIVATE KEY}), RSA or EC -- what OpenSSL 1.1.1+ and cert-manager write. */
    private PrivateKey privateKey(byte[] pem) throws GeneralSecurityException {
        String text = new String(pem, StandardCharsets.US_ASCII);
        if (!text.contains("-----BEGIN PRIVATE KEY-----")) {
            throw new InvalidKeySpecException(keyFile + " is not a PKCS#8 PEM key (BEGIN PRIVATE KEY)");
        }
        String base64 = text.replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "").replaceAll("\\s", "");
        byte[] der;
        try {
            der = Base64.getDecoder().decode(base64);
        } catch (IllegalArgumentException e) {
            throw new InvalidKeySpecException(keyFile + " is not complete or not valid base64", e);
        }
        InvalidKeySpecException last = null;
        for (String algorithm : new String[] {"RSA", "EC"}) {
            try {
                return KeyFactory.getInstance(algorithm).generatePrivate(new PKCS8EncodedKeySpec(der));
            } catch (InvalidKeySpecException e) {
                last = e;
            }
        }
        throw new InvalidKeySpecException(keyFile + " holds neither an RSA nor an EC key", last);
    }

    /** Signs with the key and verifies with the certificate: the only test that cannot be fooled by names. */
    private void requireKeyBelongsTo(PrivateKey key, X509Certificate cert) throws GeneralSecurityException {
        String algorithm = "RSA".equals(key.getAlgorithm()) ? "SHA256withRSA" : "SHA256withECDSA";
        byte[] probe = "fbtberger-raft".getBytes(StandardCharsets.US_ASCII);
        boolean matches;
        try {
            Signature signer = Signature.getInstance(algorithm);
            signer.initSign(key);
            signer.update(probe);
            byte[] signature = signer.sign();
            Signature verifier = Signature.getInstance(algorithm);
            verifier.initVerify(cert.getPublicKey());
            verifier.update(probe);
            matches = verifier.verify(signature);
        } catch (GeneralSecurityException e) {
            matches = false;
        }
        if (!matches) {
            throw new GeneralSecurityException("the key in " + keyFile + " does not belong to the certificate in "
                    + certFile + " (a renewal caught half-way writes exactly this)");
        }
    }

    private static final class Loaded {
        X509Certificate[] chain;
        X509ExtendedKeyManager keyManager;
        X509ExtendedTrustManager trustManager;
        byte[] fingerprint;
    }

    private static final class ReloadingKeyManager extends X509ExtendedKeyManager {
        volatile X509ExtendedKeyManager delegate;

        @Override public String[] getClientAliases(String keyType, Principal[] issuers) {
            return delegate.getClientAliases(keyType, issuers);
        }
        @Override public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket socket) {
            return delegate.chooseClientAlias(keyType, issuers, socket);
        }
        @Override public String[] getServerAliases(String keyType, Principal[] issuers) {
            return delegate.getServerAliases(keyType, issuers);
        }
        @Override public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
            return delegate.chooseServerAlias(keyType, issuers, socket);
        }
        @Override public X509Certificate[] getCertificateChain(String alias) {
            return delegate.getCertificateChain(alias);
        }
        @Override public PrivateKey getPrivateKey(String alias) {
            return delegate.getPrivateKey(alias);
        }
        @Override public String chooseEngineClientAlias(String[] keyType, Principal[] issuers, SSLEngine engine) {
            return delegate.chooseEngineClientAlias(keyType, issuers, engine);
        }
        @Override public String chooseEngineServerAlias(String keyType, Principal[] issuers, SSLEngine engine) {
            return delegate.chooseEngineServerAlias(keyType, issuers, engine);
        }
    }

    private static final class ReloadingTrustManager extends X509ExtendedTrustManager {
        volatile X509ExtendedTrustManager delegate;

        @Override public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket)
                throws CertificateException {
            delegate.checkClientTrusted(chain, authType, socket);
        }
        @Override public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket)
                throws CertificateException {
            delegate.checkServerTrusted(chain, authType, socket);
        }
        @Override public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
                throws CertificateException {
            delegate.checkClientTrusted(chain, authType, engine);
        }
        @Override public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine)
                throws CertificateException {
            delegate.checkServerTrusted(chain, authType, engine);
        }
        @Override public void checkClientTrusted(X509Certificate[] chain, String authType)
                throws CertificateException {
            delegate.checkClientTrusted(chain, authType);
        }
        @Override public void checkServerTrusted(X509Certificate[] chain, String authType)
                throws CertificateException {
            delegate.checkServerTrusted(chain, authType);
        }
        @Override public X509Certificate[] getAcceptedIssuers() {
            return delegate.getAcceptedIssuers();
        }
    }
}
