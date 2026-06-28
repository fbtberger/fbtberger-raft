/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.transport.RaftTransportFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

@Configuration
class RaftNodeTestConfiguration {

    @Bean
    public RaftConfig raftConfig() throws Exception {
        Properties props = new Properties();
        props.setProperty("node.id", "test-node");
        props.setProperty("node.port", "19999");
        props.setProperty("data.dir", "/tmp/raft-spring-test-unused");
        props.setProperty("peer.test-node", "localhost:19999");
        props.setProperty("snapshot.threshold", "10");

        Path tmp = Files.createTempFile("raft-spring-test-", ".properties");
        try (OutputStream out = Files.newOutputStream(tmp)) {
            props.store(out, null);
        }
        return RaftConfig.load(tmp);
    }

    @Bean
    public RaftStorage raftStorage() {
        return new InMemoryStorage();
    }

    @Bean
    public KeyValueStateMachine stateMachine() {
        return new KeyValueStateMachine();
    }

    @Bean
    public RaftTransportFactory transportFactory() {
        return address -> null;
    }

    @Bean(destroyMethod = "shutdown")
    public RaftNode raftNode(RaftConfig config,
                              RaftStorage storage,
                              KeyValueStateMachine stateMachine,
                              RaftTransportFactory transportFactory) {
        return new RaftNode(config, storage, stateMachine, transportFactory, RaftMetrics.noop());
    }
}
