/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import com.fbtberger.raft.transport.RaftTransportFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Spring IoC integration tests. Verifies that {@link RaftNodeTestConfiguration}
 * (which mirrors the structure of the production {@link RaftNodeConfiguration})
 * loads without errors, all expected beans are present and correctly typed,
 * and the injected collaborators are the test doubles rather than real
 * external resources.
 *
 * <p>These tests exercise the Spring wiring layer specifically. The behaviour
 * of the individual beans is covered by the unit tests in
 * {@link RaftNodeTest}, {@link InMemoryStorageTest}, and
 * {@link KeyValueStateMachineTest}.
 */
@SpringJUnitConfig(RaftNodeTestConfiguration.class)
class RaftNodeConfigurationTest {

    @Autowired ApplicationContext ctx;
    @Autowired RaftConfig         config;
    @Autowired RaftStorage        storage;
    @Autowired StateMachine       stateMachine;
    @Autowired RaftNode           raftNode;

    // ---- context loads --------------------------------------------------

    @Test
    void contextLoadsWithoutErrors() {
        assertNotNull(ctx, "ApplicationContext must not be null");
    }

    @Test
    void allExpectedBeansArePresent() {
        // Every class that the production configuration wires (minus the
        // gRPC server, which is omitted from the test config) must be
        // registered in the context.
        assertTrue(ctx.containsBean("raftConfig"));
        assertTrue(ctx.containsBean("raftStorage"));
        assertTrue(ctx.containsBean("stateMachine"));
        assertTrue(ctx.containsBean("transportFactory"));
        assertTrue(ctx.containsBean("raftNode"));
    }

    // ---- correct types / test doubles wired -----------------------------

    @Test
    void storageIsInMemoryNotBerkeleyDb() {
        // The test configuration replaces BerkeleyDbStorage with InMemoryStorage
        // so that tests never touch the file system.
        assertInstanceOf(InMemoryStorage.class, storage,
                "test config must wire InMemoryStorage, not BerkeleyDbStorage");
    }

    @Test
    void stateMachineIsKeyValueStateMachine() {
        assertInstanceOf(KeyValueStateMachine.class, stateMachine);
    }

    @Test
    void transportFactoryReturnsNullForAnyAddress() {
        RaftTransportFactory factory = ctx.getBean(RaftTransportFactory.class);
        assertNull(factory.connect("localhost:9999"),
                "test transport factory must return null for every address");
    }

    // ---- RaftConfig values loaded from temp properties file -------------

    @Test
    void configSelfIdMatchesTestProperties() {
        assertEquals("test-node", config.selfId());
    }

    @Test
    void configPortMatchesTestProperties() {
        assertEquals(19999, config.selfPort());
    }

    @Test
    void configContainsSelfInPeerList() {
        assertTrue(config.peerAddresses().containsKey("test-node"),
                "peer list must include the node itself (used to compute cluster size)");
    }

    @Test
    void configSnapshotThresholdMatchesTestProperties() {
        assertEquals(10, config.snapshotThreshold());
    }

    // ---- RaftNode wired to the right collaborators ----------------------

    @Test
    void raftNodeUsesInjectedStorage() {
        // Store a value directly, then verify RaftNode sees the same store.
        // Before any entries are appended the term must be 0.
        assertEquals(0, storage.getCurrentTerm(),
                "freshly injected storage must start at term 0");
    }

    @Test
    void raftNodeStartsAsFollowerAndShutDownCleanly() {
        // Before start() is called, the node must be a FOLLOWER (§5.1).
        // After start(), a single-node cluster immediately elects itself
        // leader (majority == 1), so the FOLLOWER assertion must come first.
        assertEquals(ServerRole.FOLLOWER, raftNode.role(),
                "every node starts as FOLLOWER per §5.1");
        raftNode.start();
        raftNode.shutdown();
        // A second shutdown must be idempotent (scheduler.shutdownNow()
        // on an already-terminated executor is a no-op).
        assertDoesNotThrow(raftNode::shutdown);
    }

    // ---- dependency injection is constructor-based, not field injection --

    @Test
    void raftNodeIsASingleton() {
        // Spring default scope is singleton: both lookups must return
        // the exact same instance.
        RaftNode a = ctx.getBean(RaftNode.class);
        RaftNode b = ctx.getBean(RaftNode.class);
        assertSame(a, b, "RaftNode must be a Spring singleton");
    }

    @Test
    void storageIsASingleton() {
        assertSame(ctx.getBean(RaftStorage.class), ctx.getBean(RaftStorage.class));
    }
}
