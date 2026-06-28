/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.util.Set;

public interface RaftNodeMXBean {
    String getRole();
    String getCurrentLeaderId();
    long getCurrentTerm();
    long getCommitIndex();
    long getLastApplied();
    long getLastLogIndex();
    long getSnapshotIndex();
    int getClusterSize();
    Set<String> getClusterMembers();
    boolean isLeaderTransferInProgress();

    void triggerSnapshot();
    void transferLeadership(String targetId);
}
