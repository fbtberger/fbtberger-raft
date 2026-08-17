/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.io.File;
import java.nio.file.Path;

/**
 * {@link RaftStorageContract} against {@link WalStorage}, the segmented write-ahead log.
 *
 * <p>WAL-specific behaviour that the contract cannot reach -- segment rollover, truncation across a
 * segment boundary, recovery over several segments -- lives in {@link WalStorageSegmentTest}.
 */
class WalStorageContractTest extends RaftStorageContract {

    @Override
    protected RaftStorage create(Path dir) {
        return new WalStorage(new File(dir.toFile(), "wal"));
    }

    @Override
    protected boolean isDurable() {
        return true;
    }
}
