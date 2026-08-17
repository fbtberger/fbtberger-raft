/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * A toy replicated key-value store, just to have something concrete for
 * the demo CLI to drive through Raft. Commands are UTF-8 text of the form
 * "SET key value"; GET is handled locally by RaftServer and never goes
 * through the log.
 */
public final class KeyValueStateMachine implements StateMachine {

    private final Map<String, String> data = new ConcurrentHashMap<>();

    @Override
    public byte[] apply(byte[] command) {
        if (command.length == 0) {
            // The blank no-op entry every new leader commits at the start
            // of its term (§8) ends up here; there's nothing to do with it.
            return new byte[0];
        }
        String cmd = new String(command, StandardCharsets.UTF_8);
        String[] parts = cmd.split(" ", 3);
        if (parts.length == 3 && parts[0].equals("SET")) {
            data.put(parts[1], parts[2]);
            return "OK".getBytes(StandardCharsets.UTF_8);
        }
        return ("ERR unknown command: " + cmd).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Encodes the entire map as a count followed by UTF-8 key/value pairs
     * (§7). Simple rather than compact -- fine for this demo store, where a
     * snapshot only ever needs to round-trip through {@link #restoreSnapshot}
     * on the same kind of JVM that wrote it.
     */
    @Override
    public byte[] takeSnapshot() {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(data.size());
            for (Map.Entry<String, String> entry : data.entrySet()) {
                dos.writeUTF(entry.getKey());
                dos.writeUTF(entry.getValue());
            }
            return out.toByteArray();
        } catch (IOException e) {
            // ByteArrayOutputStream/DataOutputStream never actually throw;
            // this only exists to satisfy writeUTF's checked signature.
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void restoreSnapshot(byte[] snapshot) {
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(snapshot));
            int count = dis.readInt();
            Map<String, String> restored = new ConcurrentHashMap<>();
            for (int i = 0; i < count; i++) {
                restored.put(dis.readUTF(), dis.readUTF());
            }
            data.clear();
            data.putAll(restored);
        } catch (IOException e) {
            throw new IllegalArgumentException("corrupt key-value snapshot", e);
        }
    }

    @Override
    public Supplier<byte[]> prepareCowSnapshot() {
        Map<String, String> copy = new HashMap<>(data);
        return () -> serialize(copy);
    }

    private static byte[] serialize(Map<String, String> map) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            dos.writeInt(map.size());
            for (Map.Entry<String, String> entry : map.entrySet()) {
                dos.writeUTF(entry.getKey());
                dos.writeUTF(entry.getValue());
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public String get(String key) {
        return data.get(key);
    }
}
