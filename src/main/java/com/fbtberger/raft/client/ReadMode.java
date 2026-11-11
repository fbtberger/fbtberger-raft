/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 *
 * Proprietary and confidential. Unauthorised copying, distribution, modification or
 * use of this file, in source or binary form, via any medium, is prohibited without
 * the prior written permission of fbtBerger Technology.
 *
 * SPDX-License-Identifier: LicenseRef-fbtBerger-Proprietary
 */
package com.fbtberger.raft.client;

/**
 * Which of §6.4's two linearizable-read protocols a query should take.
 *
 * <p>The choice is per request rather than per cluster, so both can be measured in one
 * run against one leader. It is an enum and not a boolean because the call sites read
 * better for it, and because {@code query(payload, timeout, true)} says nothing at all
 * about what the {@code true} selects.
 */
public enum ReadMode {

    /**
     * Confirm leadership with a majority before answering. One extra round trip per
     * read, correct whatever the clocks do. The default everywhere.
     */
    READ_INDEX,

    /**
     * Answer immediately while the leader lease holds. Saves the round trip and buys
     * a timing assumption: see {@code RaftNode#queryWithLease}, which also says what
     * this is <em>not</em> -- it is not a way to read stale data on demand, because
     * the lease falls back to ReadIndex the moment it is not held.
     */
    LEASE
}
