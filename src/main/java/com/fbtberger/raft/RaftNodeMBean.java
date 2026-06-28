/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.util.Set;

final class RaftNodeMBean implements RaftNodeMXBean {

    private final RaftNode node;
    private final RaftStorage store;

    RaftNodeMBean(RaftNode node, RaftStorage store) {
        this.node = node;
        this.store = store;
    }

    @Override public String getRole()                    { return node.role().name(); }
    @Override public String getCurrentLeaderId()         { return node.currentLeaderId(); }
    @Override public long getCurrentTerm()                { return store.getCurrentTerm(); }
    @Override public long getCommitIndex()                { return store.getLastLogIndex(); }
    @Override public long getLastApplied()                { return store.getLastLogIndex(); }
    @Override public long getLastLogIndex()               { return store.getLastLogIndex(); }
    @Override public long getSnapshotIndex()              { return node.snapshotIndex(); }
    @Override public int getClusterSize()                 { return node.currentConfiguration().size(); }
    @Override public Set<String> getClusterMembers()      { return node.currentConfiguration().keySet(); }
    @Override public boolean isLeaderTransferInProgress() { return node.isTransferInProgress(); }

    @Override
    public void triggerSnapshot() {
        node.snapshotNow();
    }

    @Override
    public void transferLeadership(String targetId) {
        node.transferLeadership(targetId);
    }
}
