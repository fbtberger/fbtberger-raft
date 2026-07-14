/*
 * Copyright 2026 fbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.io.File;
import java.nio.file.Path;

/**
 * {@link RaftStorageContract} against {@link BerkeleyDbStorage} -- the durable implementation the
 * cluster actually runs on, and the one that had no tests at all while
 * {@link BerkeleyDbStorage#truncateFrom(long)} failed on every single call.
 *
 * <p>Runs against a real Berkeley DB environment in a temp directory, so transactions and cursors
 * are exercised for real. That is the whole point: the bug was in cursor/commit ordering, which no
 * in-memory stand-in can reproduce.
 */
class BerkeleyDbStorageContractTest extends RaftStorageContract {

    @Override
    protected RaftStorage create(Path dir) {
        return new BerkeleyDbStorage(new File(dir.toFile(), "bdb"));
    }

    @Override
    protected boolean isDurable() {
        return true;
    }
}
