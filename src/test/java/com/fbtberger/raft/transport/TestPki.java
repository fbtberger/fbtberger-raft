/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Date;

/**
 * A throwaway cluster PKI for tests — one CA, node certificates issued from it.
 *
 * <p>This exists because {@code SelfSignedCertificate} cannot express the certificate shape the
 * peer transport actually needs, and using it hid a real problem. A node certificate has to carry
 * <b>two</b> different things at once:
 *
 * <ul>
 *   <li>the <b>node id in the CN</b>, which is what {@link PeerIdentity} binds the sender id to;</li>
 *   <li>the <b>address in a SAN</b>, because TLS hostname verification checks the peer certificate
 *       against the target the client dialled — and peers are dialled by IP or by Compose service
 *       name, neither of which is the node id.</li>
 * </ul>
 *
 * <p>Put the node id in the SAN instead and the handshake fails against every real peer address;
 * omit the SAN and it fails too. That the first version of the binding test connected to
 * {@code localhost} with a {@code CN=kwatro1} certificate and died in {@code HostnameChecker} is
 * exactly this, found early. A production PKI must issue the same shape: {@code CN=<nodeId>} plus
 * SANs for every address that node is reachable at.
 *
 * <p>Keys are RSA-2048 and certificates are written as PEM into a temp directory, because that is
 * what {@link TlsConfig} takes — files, not keystores.
 */
public final class TestPki implements AutoCloseable {

    private final Path dir;
    private final KeyPair caKeys;
    private final X509Certificate caCert;
    private final File caFile;

    private TestPki(String caName) throws Exception {
        this.dir = Files.createTempDirectory("raft-test-pki");
        this.caKeys = rsa();
        this.caCert = new JcaX509CertificateConverter().getCertificate(
                new JcaX509v3CertificateBuilder(
                        new X500Name("CN=" + caName), BigInteger.ONE,
                        new Date(System.currentTimeMillis() - 86_400_000L),
                        new Date(System.currentTimeMillis() + 86_400_000L),
                        new X500Name("CN=" + caName), caKeys.getPublic())
                        .addExtension(Extension.basicConstraints, true, new BasicConstraints(true))
                        .build(new JcaContentSignerBuilder("SHA256WithRSA").build(caKeys.getPrivate())));
        this.caFile = writePem("ca", "CERTIFICATE", caCert.getEncoded());
    }

    public static TestPki create(String caName) throws Exception {
        return new TestPki(caName);
    }

    /** The trust anchor both ends are configured with. */
    public File caFile() {
        return caFile;
    }

    /**
     * A node certificate: {@code CN=nodeId}, plus SANs for {@code localhost} and {@code 127.0.0.1}
     * so a test can actually dial it. Marked for both server and client auth — a Raft peer is both.
     */
    public Node issue(String nodeId) throws Exception {
        KeyPair keys = rsa();
        GeneralNames sans = new GeneralNames(new GeneralName[]{
                new GeneralName(GeneralName.dNSName, "localhost"),
                new GeneralName(GeneralName.iPAddress, "127.0.0.1")});
        X509Certificate cert = new JcaX509CertificateConverter().getCertificate(
                new JcaX509v3CertificateBuilder(
                        new X500Name(caCert.getSubjectX500Principal().getName()),
                        BigInteger.valueOf(System.nanoTime()),
                        new Date(System.currentTimeMillis() - 86_400_000L),
                        new Date(System.currentTimeMillis() + 86_400_000L),
                        new X500Name("CN=" + nodeId), keys.getPublic())
                        .addExtension(Extension.basicConstraints, true, new BasicConstraints(false))
                        .addExtension(Extension.subjectAlternativeName, false, sans)
                        .addExtension(Extension.extendedKeyUsage, false,
                                new ExtendedKeyUsage(new KeyPurposeId[]{
                                        KeyPurposeId.id_kp_serverAuth, KeyPurposeId.id_kp_clientAuth}))
                        .build(new JcaContentSignerBuilder("SHA256WithRSA").build(caKeys.getPrivate())));

        return new Node(writePem(nodeId + "-cert", "CERTIFICATE", cert.getEncoded()),
                writePem(nodeId + "-key", "PRIVATE KEY", keys.getPrivate().getEncoded()),
                cert);
    }

    public record Node(File certFile, File keyFile, X509Certificate certificate) {
        /** mTLS config for this node, trusting {@code pki}'s CA. */
        public TlsConfig tls(TestPki pki) {
            return new TlsConfig(true, certFile, keyFile, pki.caFile(), true);
        }
    }

    private static KeyPair rsa() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        return gen.generateKeyPair();
    }

    /** PKCS#8 for keys ("BEGIN PRIVATE KEY"), which is the only form TlsConfig's parser accepts. */
    private File writePem(String name, String type, byte[] der) throws IOException {
        StringBuilder pem = new StringBuilder("-----BEGIN ").append(type).append("-----\n");
        String b64 = Base64.getEncoder().encodeToString(der);
        for (int i = 0; i < b64.length(); i += 64) {
            pem.append(b64, i, Math.min(i + 64, b64.length())).append('\n');
        }
        pem.append("-----END ").append(type).append("-----\n");
        File f = dir.resolve(name + ".pem").toFile();
        Files.write(f.toPath(), pem.toString().getBytes(StandardCharsets.US_ASCII));
        return f;
    }

    @Override
    public void close() throws IOException {
        try (var paths = Files.walk(dir)) {
            paths.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    /** Only needed by tests that assert on the certificate itself rather than on a connection. */
    PrivateKey caKey() {
        return caKeys.getPrivate();
    }
}
