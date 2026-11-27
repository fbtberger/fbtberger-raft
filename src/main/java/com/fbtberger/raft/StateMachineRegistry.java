/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * The state machines one {@link RaftNode} drives: the application state machine
 * and any number of named ones beside it.
 *
 * <p><b>One state machine, one domain.</b> A state machine that also has to
 * recognise and handle somebody else's commands is two state machines in one
 * class. So Raft does the routing: a log entry either is a plain command, which
 * goes to the application state machine, or carries the name of the state
 * machine it is for. No state machine ever sees a command of another.
 *
 * <p>All servers of a cluster must register the same names. A committed entry
 * for a name a server does not have cannot be applied there, and skipping it
 * would let that server's state diverge silently, so {@link #require} fails.
 *
 * <h2>Snapshot layout</h2>
 * Raft stores one opaque byte string per snapshot. Without named state machines
 * it is exactly what the application state machine produced, byte for byte as
 * before named state machines existed. With at least one it is a container:
 *
 * <pre>
 *   8 bytes   magic 00 46 42 54 53 4D 53 01
 *   repeated  2 bytes name length, name (UTF-8), 4 bytes length, snapshot bytes
 * </pre>
 *
 * The application's section has the empty name. Lengths are big-endian.
 *
 * <p>On restore, a byte string that does not start with the magic is the
 * application's own snapshot from before a named state machine was registered.
 * Every registered state machine that has no section is restored from an empty
 * byte string, which a named state machine MUST understand as "initial state".
 * A section for a name that is not registered fails the restore: that state
 * would otherwise be dropped without a word.
 */
public final class StateMachineRegistry {

    /** The name of the application state machine: the empty string. */
    public static final String APPLICATION = "";

    /** Longest allowed name, in characters. */
    public static final int MAX_NAME_LENGTH = 64;

    static final byte[] SNAPSHOT_MAGIC = {0x00, 0x46, 0x42, 0x54, 0x53, 0x4D, 0x53, 0x01};

    private static final byte[] EMPTY = new byte[0];

    private final StateMachine application;
    /** Sorted, so every server writes the sections in the same order. */
    private final Map<String, StateMachine> named;
    /** A witness's registry: every name leads to the one state machine that keeps nothing. */
    private final boolean anyName;

    public StateMachineRegistry(StateMachine application, Map<String, StateMachine> named) {
        this(application, named, false);
    }

    private StateMachineRegistry(StateMachine application, Map<String, StateMachine> named, boolean anyName) {
        this.anyName = anyName;
        this.application = Objects.requireNonNull(application, "application");
        TreeMap<String, StateMachine> copy = new TreeMap<>();
        for (Map.Entry<String, StateMachine> entry : named.entrySet()) {
            validateName(entry.getKey());
            copy.put(entry.getKey(), Objects.requireNonNull(entry.getValue(), entry.getKey()));
        }
        this.named = Collections.unmodifiableMap(copy);
    }

    /** A registry with the application state machine only. */
    public static StateMachineRegistry of(StateMachine application) {
        return new StateMachineRegistry(application, Map.of());
    }

    /**
     * The registry of a witness. A witness applies every committed entry like any
     * server and keeps no state, so whatever name an entry carries leads to
     * {@code nothing}, and {@link #names()} is empty: the witness need not be told
     * which names the data nodes registered.
     */
    static StateMachineRegistry forWitness(StateMachine nothing) {
        return new StateMachineRegistry(nothing, Map.of(), true);
    }

    /** Builds a registry from registrations; two registrations with one name are refused. */
    public static StateMachineRegistry of(StateMachine application, List<StateMachineRegistration> registrations) {
        Map<String, StateMachine> named = new LinkedHashMap<>();
        for (StateMachineRegistration registration : registrations) {
            if (named.put(registration.name(), registration.stateMachine()) != null) {
                throw new IllegalArgumentException(
                        "two state machines are registered as '" + registration.name() + "'");
            }
        }
        return new StateMachineRegistry(application, named);
    }

    /**
     * A name is 1 to {@value #MAX_NAME_LENGTH} characters out of
     * {@code a-z A-Z 0-9 . _ -}. The empty name is the application's and cannot
     * be registered.
     */
    public static void validateName(String name) {
        Objects.requireNonNull(name, "name");
        if (name.isEmpty() || name.length() > MAX_NAME_LENGTH || !name.matches("[A-Za-z0-9._-]+")) {
            throw new IllegalArgumentException("not a state machine name: '" + name
                    + "' (1 to " + MAX_NAME_LENGTH + " characters out of a-z A-Z 0-9 . _ -)");
        }
    }

    public StateMachine application() {
        return application;
    }

    /** The registered names, without the application's, in sorted order. */
    public Set<String> names() {
        return named.keySet();
    }

    /**
     * The state machine a name addresses; {@link #APPLICATION} is the application's.
     *
     * @throws UnknownStateMachineException if nothing is registered under the name
     */
    public StateMachine require(String name) {
        if (anyName || name == null || name.isEmpty()) {
            return application;
        }
        StateMachine stateMachine = named.get(name);
        if (stateMachine == null) {
            throw new UnknownStateMachineException(name,
                    "no state machine is registered as '" + name + "' on this server; registered: " + named.keySet());
        }
        return stateMachine;
    }

    // ---- snapshots ---------------------------------------------------------

    /** Snapshots every state machine now; see the class comment for the layout. */
    public byte[] takeSnapshot() {
        if (named.isEmpty()) {
            return application.takeSnapshot();
        }
        List<Supplier<byte[]>> sections = new ArrayList<>();
        byte[] applicationData = application.takeSnapshot();
        sections.add(() -> applicationData);
        for (StateMachine stateMachine : named.values()) {
            byte[] data = stateMachine.takeSnapshot();
            sections.add(() -> data);
        }
        return pack(sections);
    }

    /**
     * Captures every state machine under the Raft lock and defers the
     * serialization, with the contract of {@link StateMachine#prepareCowSnapshot}.
     */
    public Supplier<byte[]> prepareCowSnapshot() {
        if (named.isEmpty()) {
            return application.prepareCowSnapshot();
        }
        List<Supplier<byte[]>> sections = new ArrayList<>();
        sections.add(application.prepareCowSnapshot());
        for (StateMachine stateMachine : named.values()) {
            sections.add(stateMachine.prepareCowSnapshot());
        }
        return () -> pack(sections);
    }

    private byte[] pack(List<Supplier<byte[]>> sections) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(SNAPSHOT_MAGIC);
        List<String> names = new ArrayList<>();
        names.add(APPLICATION);
        names.addAll(named.keySet());
        for (int i = 0; i < names.size(); i++) {
            byte[] name = names.get(i).getBytes(StandardCharsets.UTF_8);
            byte[] data = sections.get(i).get();
            out.writeBytes(ByteBuffer.allocate(2).putShort((short) name.length).array());
            out.writeBytes(name);
            out.writeBytes(ByteBuffer.allocate(4).putInt(data.length).array());
            out.writeBytes(data);
        }
        return out.toByteArray();
    }

    /**
     * Replaces the state of every registered state machine with what the snapshot
     * holds; see the class comment for what happens to missing and unknown sections.
     *
     * @throws UnknownStateMachineException if the snapshot has a section for a name
     *                                      that is not registered
     * @throws IllegalArgumentException     if the container is cut off
     */
    public void restoreSnapshot(byte[] snapshot) {
        Map<String, byte[]> sections = isContainer(snapshot) ? unpack(snapshot) : Map.of(APPLICATION, snapshot);
        for (String name : sections.keySet()) {
            require(name);
        }
        byte[] applicationData = sections.get(APPLICATION);
        if (applicationData != null) {
            application.restoreSnapshot(applicationData);
        }
        for (Map.Entry<String, StateMachine> entry : named.entrySet()) {
            entry.getValue().restoreSnapshot(sections.getOrDefault(entry.getKey(), EMPTY));
        }
    }

    // ---- snapshots as streams ----------------------------------------------
    //
    // The same layout as above, written and read without holding a whole snapshot
    // in memory. RaftNode only uses these three.

    /** The streaming form of {@link #takeSnapshot()}. */
    public void writeSnapshot(OutputStream out) throws IOException {
        if (named.isEmpty()) {
            application.writeSnapshot(out);
            return;
        }
        List<StateMachine.SnapshotWriter> sections = new ArrayList<>();
        sections.add(application::writeSnapshot);
        for (StateMachine stateMachine : named.values()) {
            sections.add(stateMachine::writeSnapshot);
        }
        writeContainer(sections, out);
    }

    /**
     * The streaming form of {@link #prepareCowSnapshot()}, with the contract of
     * {@link StateMachine#prepareSnapshotWriter()}: every state machine is captured
     * here, under the Raft lock, and written later.
     */
    public StateMachine.SnapshotWriter prepareSnapshotWriter() {
        if (named.isEmpty()) {
            return application.prepareSnapshotWriter();
        }
        List<StateMachine.SnapshotWriter> sections = new ArrayList<>();
        sections.add(application.prepareSnapshotWriter());
        for (StateMachine stateMachine : named.values()) {
            sections.add(stateMachine.prepareSnapshotWriter());
        }
        return out -> writeContainer(sections, out);
    }

    /**
     * A section's length stands in front of it, and a state machine that streams
     * does not know its length before it has written. So each section goes to a
     * temporary file first and is copied from there: one section on disk twice for
     * a moment, none of them in memory.
     */
    private void writeContainer(List<StateMachine.SnapshotWriter> sections, OutputStream out) throws IOException {
        DataOutputStream data = new DataOutputStream(out);
        data.write(SNAPSHOT_MAGIC);
        List<String> names = new ArrayList<>();
        names.add(APPLICATION);
        names.addAll(named.keySet());
        for (int i = 0; i < names.size(); i++) {
            byte[] name = names.get(i).getBytes(StandardCharsets.UTF_8);
            Path spool = Files.createTempFile("raft-snapshot-section", ".tmp");
            try {
                try (OutputStream section = new BufferedOutputStream(Files.newOutputStream(spool))) {
                    sections.get(i).writeTo(section);
                }
                long length = Files.size(spool);
                if (length > Integer.MAX_VALUE) {
                    throw new IOException("the snapshot of state machine '" + names.get(i) + "' is " + length
                            + " bytes; a section holds at most " + Integer.MAX_VALUE);
                }
                data.writeShort(name.length);
                data.write(name);
                data.writeInt((int) length);
                Files.copy(spool, data);
            } finally {
                Files.deleteIfExists(spool);
            }
        }
        data.flush();
    }

    /**
     * The streaming form of {@link #restoreSnapshot(byte[])}, with one difference
     * that follows from reading front to back: a section for a name that is not
     * registered is only found when the stream reaches it, after the sections in
     * front of it have been restored. The restore fails all the same, and a server
     * whose restore failed must not go on to serve.
     */
    public void restoreSnapshot(InputStream snapshot) throws IOException {
        if (anyName) {
            application.restoreSnapshot(snapshot);
            return;
        }
        PushbackInputStream in = new PushbackInputStream(snapshot, SNAPSHOT_MAGIC.length);
        byte[] head = in.readNBytes(SNAPSHOT_MAGIC.length);
        if (!isContainer(head)) {
            in.unread(head);
            application.restoreSnapshot(in);
            for (StateMachine stateMachine : named.values()) {
                stateMachine.restoreSnapshot(new ByteArrayInputStream(EMPTY));
            }
            return;
        }
        DataInputStream data = new DataInputStream(in);
        Set<String> restored = new HashSet<>();
        while (true) {
            int first = data.read();
            if (first < 0) {
                break;
            }
            try {
                byte[] name = new byte[(first << 8) | data.readUnsignedByte()];
                data.readFully(name);
                int length = data.readInt();
                if (length < 0) {
                    throw new IllegalArgumentException("snapshot section is cut off");
                }
                String sectionName = new String(name, StandardCharsets.UTF_8);
                SectionInputStream section = new SectionInputStream(data, length);
                require(sectionName).restoreSnapshot(section);
                // A state machine need not read its section to the end, and one that
                // read less must not leave the next section's header out of place.
                section.skipToEnd();
                restored.add(sectionName);
            } catch (EOFException e) {
                throw new IllegalArgumentException("snapshot section is cut off", e);
            }
        }
        for (Map.Entry<String, StateMachine> entry : named.entrySet()) {
            if (!restored.contains(entry.getKey())) {
                entry.getValue().restoreSnapshot(new ByteArrayInputStream(EMPTY));
            }
        }
    }

    /** One section of a container: ends where the section ends, not where the file does. */
    private static final class SectionInputStream extends InputStream {
        private final InputStream in;
        private long remaining;

        SectionInputStream(InputStream in, long length) {
            this.in = in;
            this.remaining = length;
        }

        @Override
        public int read() throws IOException {
            if (remaining == 0) {
                return -1;
            }
            int b = in.read();
            if (b < 0) {
                throw new EOFException();
            }
            remaining--;
            return b;
        }

        @Override
        public int read(byte[] target, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            if (remaining == 0) {
                return -1;
            }
            int read = in.read(target, offset, (int) Math.min(length, remaining));
            if (read < 0) {
                throw new EOFException();
            }
            remaining -= read;
            return read;
        }

        void skipToEnd() throws IOException {
            byte[] scratch = new byte[8192];
            while (read(scratch, 0, scratch.length) >= 0) {
                // discard
            }
        }
    }

    static boolean isContainer(byte[] snapshot) {
        return snapshot.length >= SNAPSHOT_MAGIC.length
                && Arrays.equals(snapshot, 0, SNAPSHOT_MAGIC.length, SNAPSHOT_MAGIC, 0, SNAPSHOT_MAGIC.length);
    }

    private static Map<String, byte[]> unpack(byte[] snapshot) {
        Map<String, byte[]> sections = new LinkedHashMap<>();
        ByteBuffer in = ByteBuffer.wrap(snapshot);
        in.position(SNAPSHOT_MAGIC.length);
        try {
            while (in.hasRemaining()) {
                byte[] name = new byte[in.getShort() & 0xFFFF];
                in.get(name);
                int length = in.getInt();
                if (length < 0 || length > in.remaining()) {
                    throw new IllegalArgumentException("snapshot section is cut off");
                }
                byte[] data = new byte[length];
                in.get(data);
                sections.put(new String(name, StandardCharsets.UTF_8), data);
            }
        } catch (java.nio.BufferUnderflowException e) {
            throw new IllegalArgumentException("snapshot section is cut off", e);
        }
        return sections;
    }
}
