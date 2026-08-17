/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.nio.file.Path;

/**
 * {@link RaftStorageContract} against {@link InMemoryStorage}.
 *
 * <p>The only implementation allowed to answer {@code false} to {@link #isDurable()}: it keeps
 * everything in a map and forgets it on shutdown, by design. The durability invariants in the
 * contract are therefore skipped here rather than silently asserted -- which is the honest version
 * of the claim this class's predecessor used to make, that its invariants "apply equally to
 * BerkeleyDbStorage". They did not, and a truncate bug lived in the gap for months.
 */
class InMemoryStorageContractTest extends RaftStorageContract {

    @Override
    protected RaftStorage create(Path dir) {
        return new InMemoryStorage();
    }

    @Override
    protected boolean isDurable() {
        return false;
    }
}
