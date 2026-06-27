package com.fbtberger.raft;

import com.fbtberger.raft.transport.RpcTimeouts;
import com.fbtberger.raft.transport.TlsConfig;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class TlsAndTimeoutsTest {

    @Test
    void tlsDisabledByDefault() {
        TlsConfig tls = TlsConfig.fromProperties(new Properties());
        assertFalse(tls.enabled());
    }

    @Test
    void tlsDisabledExplicitly() {
        Properties props = new Properties();
        props.setProperty("tls.enabled", "false");
        TlsConfig tls = TlsConfig.fromProperties(props);
        assertFalse(tls.enabled());
    }

    @Test
    void tlsEnabledRequiresCertPaths() {
        Properties props = new Properties();
        props.setProperty("tls.enabled", "true");
        assertThrows(IllegalArgumentException.class, () -> TlsConfig.fromProperties(props));
    }

    @Test
    void tlsEnabledParsesAllPaths() {
        Properties props = new Properties();
        props.setProperty("tls.enabled", "true");
        props.setProperty("tls.cert.path", "/tmp/cert.pem");
        props.setProperty("tls.key.path", "/tmp/key.pem");
        props.setProperty("tls.ca.path", "/tmp/ca.pem");
        props.setProperty("tls.mtls.enabled", "true");
        TlsConfig tls = TlsConfig.fromProperties(props);
        assertTrue(tls.enabled());
        assertTrue(tls.mtlsEnabled());
        assertEquals("/tmp/cert.pem", tls.certFile().getPath());
    }

    @Test
    void tlsDisabledFactory() {
        TlsConfig tls = TlsConfig.disabled();
        assertFalse(tls.enabled());
        assertFalse(tls.mtlsEnabled());
    }

    @Test
    void rpcTimeoutsDefaults() {
        RpcTimeouts t = RpcTimeouts.defaults();
        assertEquals(1000, t.requestVoteMs());
        assertEquals(2000, t.appendEntriesMs());
        assertEquals(30000, t.installSnapshotMs());
        assertEquals(1000, t.preVoteMs());
    }

    @Test
    void rpcTimeoutsFromProperties() {
        Properties props = new Properties();
        props.setProperty("rpc.timeout.request.vote.ms", "500");
        props.setProperty("rpc.timeout.append.entries.ms", "750");
        props.setProperty("rpc.timeout.install.snapshot.ms", "5000");
        props.setProperty("rpc.timeout.pre.vote.ms", "600");
        RpcTimeouts t = RpcTimeouts.fromProperties(props);
        assertEquals(500, t.requestVoteMs());
        assertEquals(750, t.appendEntriesMs());
        assertEquals(5000, t.installSnapshotMs());
        assertEquals(600, t.preVoteMs());
    }

    @Test
    void rpcTimeoutsFallsBackToDefaults() {
        RpcTimeouts t = RpcTimeouts.fromProperties(new Properties());
        assertEquals(RpcTimeouts.DEFAULT_REQUEST_VOTE_MS, t.requestVoteMs());
        assertEquals(RpcTimeouts.DEFAULT_APPEND_ENTRIES_MS, t.appendEntriesMs());
    }
}
