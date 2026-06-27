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
}
