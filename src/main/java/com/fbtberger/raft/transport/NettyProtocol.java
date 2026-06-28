/*
 * Copyright 2026 FbtBerger Technology. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft.transport;

/**
 * Wire format for the Netty transport.
 *
 * <pre>
 * +----------+------+-------+---------+
 * | len (4B) | type | reqId | payload |
 * +----------+------+-------+---------+
 *   len    = total bytes after this field
 *   type   = 1 byte message type (see constants)
 *   reqId  = 4 byte request correlation id
 *   payload = protobuf-encoded request or response
 * </pre>
 */
final class NettyProtocol {
    static final byte REQUEST_VOTE_REQ       = 1;
    static final byte REQUEST_VOTE_RESP      = 2;
    static final byte APPEND_ENTRIES_REQ     = 3;
    static final byte APPEND_ENTRIES_RESP    = 4;
    static final byte INSTALL_SNAPSHOT_REQ   = 5;
    static final byte INSTALL_SNAPSHOT_RESP  = 6;
    static final byte PRE_VOTE_REQ           = 7;
    static final byte PRE_VOTE_RESP          = 8;
    static final byte TIMEOUT_NOW_REQ        = 9;
    static final byte TIMEOUT_NOW_RESP       = 10;

    static final int HEADER_BYTES = 1 + 4; // type + reqId

    private NettyProtocol() {}
}
