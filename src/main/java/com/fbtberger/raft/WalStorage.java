/*
 * Copyright 2026 fbtBerger Technology
 * SPDX-License-Identifier: Apache-2.0
 */
package com.fbtberger.raft;

import com.fbtberger.raft.proto.LogEntry;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.zip.CRC32;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link RaftStorage} backed by a segmented, append-only Write-Ahead Log.
 *
 * <p>Layout on disk:
 * <ul>
 *   <li>{@code wal-NNNNNN.log} — numbered segment files, each an append-only
 *       sequence of CRC32-checked protobuf {@link LogEntry} frames:
 *       {@code [4B length][4B CRC32][protobuf bytes]...}. A new segment is
 *       started when the active segment exceeds {@code maxSegmentBytes}.</li>
 *   <li>{@code meta} — currentTerm (8B) + votedFor length (4B) + votedFor
 *       UTF-8 bytes; atomically replaced on each update via rename</li>
 *   <li>{@code snapshot} — snapshot metadata + payload; atomically
 *       replaced via rename</li>
 * </ul>
 */
public final class WalStorage implements RaftStorage {

    static final int FRAME_HEADER_SIZE = 8; // 4B length + 4B CRC32
    static final long DEFAULT_MAX_SEGMENT_BYTES = 64 * 1024 * 1024; // 64 MB
    private static final Pattern SEGMENT_PATTERN = Pattern.compile("wal-(\\d{6})\\.log");

    private final File dataDir;
    private final File metaFile;
    private final File snapshotFile;
    private final long maxSegmentBytes;
    private final ExecutorService syncExecutor;

    private final TreeMap<Integer, File> segmentFiles = new TreeMap<>();
    private int activeSegmentId;
    private RandomAccessFile activeSegment;
    private final NavigableMap<Long, WalEntry> index = new TreeMap<>();

    private long currentTerm;
    private String votedFor;
    /** The stored snapshot without its payload, which stays in {@link #snapshotFile}. */
    private SnapshotMeta snapshotMeta;
    private final java.util.concurrent.atomic.AtomicLong snapshotTmpCounter = new java.util.concurrent.atomic.AtomicLong();
    private long lastLogIndex;
    private long lastLogTerm;

    private record WalEntry(int segmentId, long fileOffset, int length) {}

    public WalStorage(File dataDir) {
        this(dataDir, DEFAULT_MAX_SEGMENT_BYTES);
    }

    public WalStorage(File dataDir, long maxSegmentBytes) {
        this.dataDir = dataDir;
        this.metaFile = new File(dataDir, "meta");
        this.snapshotFile = new File(dataDir, "snapshot");
        this.maxSegmentBytes = maxSegmentBytes;
        this.syncExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "wal-sync");
            t.setDaemon(true);
            return t;
        });

        if (!dataDir.exists() && !dataDir.mkdirs()) {
            throw new IllegalStateException("could not create WAL dir: " + dataDir);
        }

        try {
            recoverMeta();
            recoverSnapshot();
            migrateOldWalIfNeeded();
            discoverSegments();
            if (segmentFiles.isEmpty()) {
                createSegment(1);
            } else {
                activeSegmentId = segmentFiles.lastKey();
                activeSegment = new RandomAccessFile(segmentFiles.get(activeSegmentId), "rw");
            }
            recoverAllSegments();
        } catch (IOException e) {
            throw new UncheckedIOException("WAL recovery failed", e);
        }
    }

    // ---- metadata --------------------------------------------------------

    @Override
    public synchronized long getCurrentTerm() { return currentTerm; }

    @Override
    public synchronized String getVotedFor() { return votedFor; }

    @Override
    public synchronized void setTermAndVote(long term, String votedFor) {
        this.currentTerm = term;
        this.votedFor = votedFor;
        persistMeta();
    }

    private void persistMeta() {
        File tmp = new File(dataDir, "meta.tmp");
        try (FileOutputStream fos = new FileOutputStream(tmp);
             DataOutputStream dos = new DataOutputStream(fos)) {
            dos.writeLong(currentTerm);
            if (votedFor == null) {
                dos.writeInt(-1);
            } else {
                byte[] bytes = votedFor.getBytes(StandardCharsets.UTF_8);
                dos.writeInt(bytes.length);
                dos.write(bytes);
            }
            dos.flush();
            fos.getFD().sync();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (!tmp.renameTo(metaFile)) {
            throw new UncheckedIOException(new IOException("rename meta.tmp -> meta failed"));
        }
    }

    private void recoverMeta() throws IOException {
        if (!metaFile.exists()) return;
        try (DataInputStream dis = new DataInputStream(new FileInputStream(metaFile))) {
            this.currentTerm = dis.readLong();
            int len = dis.readInt();
            this.votedFor = len < 0 ? null : new String(dis.readNBytes(len), StandardCharsets.UTF_8);
        }
    }

    // ---- log (WAL) -------------------------------------------------------

    @Override
    public synchronized LogEntry getLogEntry(long idx) {
        WalEntry we = index.get(idx);
        if (we == null) return null;
        try {
            return readEntry(we);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private LogEntry readEntry(WalEntry we) throws IOException {
        if (we.segmentId == activeSegmentId) {
            long savedPos = activeSegment.getFilePointer();
            activeSegment.seek(we.fileOffset);
            byte[] buf = new byte[we.length];
            activeSegment.readFully(buf);
            activeSegment.seek(savedPos);
            return LogEntry.parseFrom(buf);
        }
        File segFile = segmentFiles.get(we.segmentId);
        if (segFile == null) return null;
        try (RandomAccessFile raf = new RandomAccessFile(segFile, "r")) {
            raf.seek(we.fileOffset);
            byte[] buf = new byte[we.length];
            raf.readFully(buf);
            return LogEntry.parseFrom(buf);
        }
    }

    @Override
    public synchronized long getLastLogIndex() { return lastLogIndex; }

    @Override
    public synchronized long getLastLogTerm() { return lastLogTerm; }

    @Override
    public synchronized void appendEntries(Iterable<LogEntry> entries) {
        appendToWal(entries);
        syncActiveSegment();
    }

    @Override
    public synchronized CompletableFuture<Void> appendEntriesDeferSync(Iterable<LogEntry> entries) {
        appendToWal(entries);
        return CompletableFuture.runAsync(this::syncActiveSegment, syncExecutor);
    }

    private void appendToWal(Iterable<LogEntry> entries) {
        try {
            for (LogEntry entry : entries) {
                maybeRotateSegment();
                byte[] bytes = entry.toByteArray();
                long entryOffset = activeSegment.length();
                activeSegment.seek(entryOffset);
                activeSegment.writeInt(bytes.length);
                activeSegment.writeInt(crc32(bytes));
                activeSegment.write(bytes);
                index.put(entry.getIndex(), new WalEntry(activeSegmentId,
                        entryOffset + FRAME_HEADER_SIZE, bytes.length));
                lastLogIndex = entry.getIndex();
                lastLogTerm = entry.getTerm();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void maybeRotateSegment() throws IOException {
        if (activeSegment.length() >= maxSegmentBytes) {
            syncActiveSegment();
            activeSegment.close();
            createSegment(activeSegmentId + 1);
        }
    }

    private void createSegment(int segId) throws IOException {
        File segFile = segmentFile(segId);
        segmentFiles.put(segId, segFile);
        activeSegmentId = segId;
        activeSegment = new RandomAccessFile(segFile, "rw");
    }

    private File segmentFile(int segId) {
        return new File(dataDir, String.format("wal-%06d.log", segId));
    }

    static int crc32(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return (int) crc.getValue();
    }

    private synchronized void syncActiveSegment() {
        try {
            activeSegment.getFD().sync();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void truncateFrom(long fromIndexInclusive) {
        NavigableMap<Long, WalEntry> tail = index.tailMap(fromIndexInclusive, true);
        if (tail.isEmpty()) return;

        WalEntry first = tail.firstEntry().getValue();
        int firstSegId = first.segmentId;
        long truncateOffset = first.fileOffset - FRAME_HEADER_SIZE;
        tail.clear();

        try {
            // Delete all segments after the one containing the truncation point
            List<Integer> toRemove = new ArrayList<>(segmentFiles.tailMap(firstSegId, false).keySet());
            for (int segId : toRemove) {
                if (segId == activeSegmentId) {
                    activeSegment.close();
                }
                segmentFiles.remove(segId).delete();
            }

            // Truncate the segment containing the first removed entry
            if (firstSegId == activeSegmentId) {
                activeSegment.setLength(truncateOffset);
            } else {
                activeSegment.close();
                try (RandomAccessFile raf = new RandomAccessFile(segmentFiles.get(firstSegId), "rw")) {
                    raf.setLength(truncateOffset);
                }
                activeSegmentId = firstSegId;
                activeSegment = new RandomAccessFile(segmentFiles.get(firstSegId), "rw");
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (index.isEmpty()) {
            lastLogIndex = snapshotMeta != null ? snapshotMeta.lastIncludedIndex : 0;
            lastLogTerm = snapshotMeta != null ? snapshotMeta.lastIncludedTerm : 0;
        } else {
            var last = index.lastEntry();
            lastLogIndex = last.getKey();
            LogEntry entry = getLogEntry(lastLogIndex);
            lastLogTerm = entry != null ? entry.getTerm() : 0;
        }
    }

    // ---- snapshotting (§7) -----------------------------------------------
    //
    // One file, `snapshot`:
    //   [8 lastIncludedIndex][8 lastIncludedTerm][4 payloadLength][payload]
    //   [4 configLength][config][4 sessionLength][sessions]
    //
    // Only the metadata is held in memory. The payload is written to the file as a
    // stream and read back from it as a stream or a chunk at a time, so its size is a
    // question of disk and not of heap. The 4-byte length bounds a payload at 2 GiB.

    /** Where the payload starts in the snapshot file. */
    private static final int SNAPSHOT_PAYLOAD_OFFSET = Long.BYTES + Long.BYTES + Integer.BYTES;

    @Override
    public synchronized long getSnapshotIndex() {
        return snapshotMeta != null ? snapshotMeta.lastIncludedIndex : 0;
    }

    @Override
    public synchronized long getSnapshotTerm() {
        return snapshotMeta != null ? snapshotMeta.lastIncludedTerm : 0;
    }

    @Override
    public synchronized SnapshotMeta getSnapshotMeta() {
        return snapshotMeta;
    }

    /** The whole snapshot in memory. Kept for callers of the array API; RaftNode does not use it. */
    @Override
    public synchronized Snapshot getSnapshot() {
        if (snapshotMeta == null) return null;
        byte[] payload = new byte[(int) snapshotMeta.dataLength];
        try (RandomAccessFile file = new RandomAccessFile(snapshotFile, "r")) {
            file.seek(SNAPSHOT_PAYLOAD_OFFSET);
            file.readFully(payload);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new Snapshot(snapshotMeta.lastIncludedIndex, snapshotMeta.lastIncludedTerm, payload,
                snapshotMeta.configurationData, snapshotMeta.sessionData);
    }

    @Override
    public synchronized java.io.InputStream openSnapshotData() throws IOException {
        if (snapshotMeta == null) return null;
        // The descriptor keeps pointing at this snapshot even if a newer one is renamed
        // over the path while the caller is still reading.
        FileInputStream in = new FileInputStream(snapshotFile);
        try {
            in.skipNBytes(SNAPSHOT_PAYLOAD_OFFSET);
        } catch (IOException e) {
            in.close();
            throw e;
        }
        return new BoundedInputStream(new java.io.BufferedInputStream(in, 1 << 16), snapshotMeta.dataLength);
    }

    @Override
    public synchronized int readSnapshotData(long lastIncludedIndex, long offset, byte[] target,
                                             int targetOffset, int length) throws IOException {
        if (snapshotMeta == null || snapshotMeta.lastIncludedIndex != lastIncludedIndex) {
            return -1;
        }
        int available = (int) Math.max(0, Math.min(length, snapshotMeta.dataLength - offset));
        if (available == 0) return 0;
        try (RandomAccessFile file = new RandomAccessFile(snapshotFile, "r")) {
            file.seek(SNAPSHOT_PAYLOAD_OFFSET + offset);
            file.readFully(target, targetOffset, available);
        }
        return available;
    }

    @Override
    public void saveSnapshotAndCompact(Snapshot snap) {
        try (SnapshotSink sink = beginSnapshot(snap.lastIncludedIndex, snap.lastIncludedTerm)) {
            sink.stream().write(snap.stateMachineData);
            sink.commit(snap.configurationData, snap.sessionData);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Writes into a temporary file of its own, so several snapshots can be in progress --
     * a background one and one being installed from the leader -- without seeing each
     * other, and the stored snapshot is untouched until {@code commit}.
     */
    @Override
    public SnapshotSink beginSnapshot(long lastIncludedIndex, long lastIncludedTerm) throws IOException {
        File tmp = new File(dataDir, "snapshot.tmp." + snapshotTmpCounter.incrementAndGet());
        RandomAccessFile file = new RandomAccessFile(tmp, "rw");
        file.writeLong(lastIncludedIndex);
        file.writeLong(lastIncludedTerm);
        file.writeInt(0); // payload length, patched in at commit when it is known
        java.io.OutputStream payload =
                new java.io.BufferedOutputStream(new FileOutputStream(file.getFD()), 1 << 16);
        return new SnapshotSink() {
            private boolean finished;

            @Override public java.io.OutputStream stream() { return payload; }

            @Override
            public void commit(byte[] configurationData, byte[] sessionData) throws IOException {
                payload.flush();
                long payloadLength = file.length() - SNAPSHOT_PAYLOAD_OFFSET;
                if (payloadLength > Integer.MAX_VALUE) {
                    throw new IOException("snapshot payload of " + payloadLength
                            + " bytes exceeds the 2 GiB this file format can describe");
                }
                file.seek(file.length());
                file.writeInt(configurationData.length);
                file.write(configurationData);
                // Appended, never inserted: a snapshot file written before the session
                // table existed simply ends before this, and recovery reads that as empty.
                file.writeInt(sessionData.length);
                file.write(sessionData);
                file.seek(Long.BYTES + Long.BYTES);
                file.writeInt((int) payloadLength);
                file.getFD().sync();
                file.close();
                finished = true;
                install(tmp, new SnapshotMeta(lastIncludedIndex, lastIncludedTerm,
                        configurationData, sessionData, payloadLength));
            }

            @Override
            public void close() {
                if (finished) return;
                finished = true;
                try {
                    file.close();
                } catch (IOException ignored) {
                    // the file is being thrown away
                }
                tmp.delete();
            }
        };
    }

    private synchronized void install(File tmp, SnapshotMeta meta) {
        // The snapshot boundary must never move backwards: a background (COW)
        // snapshot in RaftNode decides to save off-lock, so if a newer snapshot
        // (e.g. one just installed by InstallSnapshot after a step-down/
        // re-election) landed in between, that stale save is dropped here rather
        // than overwriting the higher boundary and rewinding the compaction.
        if (snapshotMeta != null && meta.lastIncludedIndex <= snapshotMeta.lastIncludedIndex) {
            tmp.delete();
            return;
        }
        if (!tmp.renameTo(snapshotFile)) {
            tmp.delete();
            throw new UncheckedIOException(new IOException("rename " + tmp.getName() + " -> snapshot failed"));
        }
        this.snapshotMeta = meta;

        index.headMap(meta.lastIncludedIndex, true).clear();

        if (lastLogIndex <= meta.lastIncludedIndex) {
            lastLogIndex = meta.lastIncludedIndex;
            lastLogTerm = meta.lastIncludedTerm;
        }

        deleteObsoleteSegments();
    }

    private void deleteObsoleteSegments() {
        List<Integer> toDelete = new ArrayList<>();
        for (var entry : segmentFiles.entrySet()) {
            int segId = entry.getKey();
            if (segId == activeSegmentId) continue;
            boolean hasLiveEntries = index.values().stream()
                    .anyMatch(we -> we.segmentId == segId);
            if (!hasLiveEntries) {
                toDelete.add(segId);
            }
        }
        for (int segId : toDelete) {
            segmentFiles.remove(segId).delete();
        }
    }

    // ---- term-at / recovery ----------------------------------------------

    @Override
    public synchronized long getTermAt(long idx) {
        if (idx <= 0) return 0L;
        if (snapshotMeta != null && idx == snapshotMeta.lastIncludedIndex) return snapshotMeta.lastIncludedTerm;
        LogEntry entry = getLogEntry(idx);
        return entry == null ? -1L : entry.getTerm();
    }

    private void recoverSnapshot() throws IOException {
        // A snapshot that was being written when the process died never became the
        // stored one; its file is only litter.
        File[] leftovers = dataDir.listFiles((dir, name) -> name.startsWith("snapshot.tmp"));
        if (leftovers != null) {
            for (File leftover : leftovers) leftover.delete();
        }
        if (!snapshotFile.exists()) return;
        try (DataInputStream dis = new DataInputStream(
                new java.io.BufferedInputStream(new FileInputStream(snapshotFile), 1 << 16))) {
            long lastIdx = dis.readLong();
            long lastTerm = dis.readLong();
            int payloadLength = dis.readInt();
            dis.skipNBytes(payloadLength); // the payload stays on disk
            int cfgLen = dis.readInt();
            byte[] cfgData = dis.readNBytes(cfgLen);
            byte[] sessionData = new byte[0];
            if (dis.available() >= Integer.BYTES) {
                sessionData = dis.readNBytes(dis.readInt());
            }
            this.snapshotMeta = new SnapshotMeta(lastIdx, lastTerm, cfgData, sessionData, payloadLength);
            this.lastLogIndex = lastIdx;
            this.lastLogTerm = lastTerm;
        }
    }

    private void migrateOldWalIfNeeded() {
        File oldWal = new File(dataDir, "wal.log");
        if (oldWal.exists() && oldWal.length() > 0) {
            File newName = segmentFile(1);
            if (!oldWal.renameTo(newName)) {
                throw new UncheckedIOException(
                        new IOException("failed to migrate wal.log -> " + newName.getName()));
            }
        } else if (oldWal.exists()) {
            oldWal.delete();
        }
    }

    private void discoverSegments() {
        File[] files = dataDir.listFiles();
        if (files == null) return;
        for (File f : files) {
            Matcher m = SEGMENT_PATTERN.matcher(f.getName());
            if (m.matches()) {
                segmentFiles.put(Integer.parseInt(m.group(1)), f);
            }
        }
    }

    private void recoverAllSegments() throws IOException {
        for (var entry : segmentFiles.entrySet()) {
            int segId = entry.getKey();
            File segFile = entry.getValue();
            if (segId == activeSegmentId) {
                recoverSegment(segId, activeSegment);
            } else {
                try (RandomAccessFile raf = new RandomAccessFile(segFile, "rw")) {
                    recoverSegment(segId, raf);
                }
            }
        }
    }

    private void recoverSegment(int segId, RandomAccessFile raf) throws IOException {
        long fileLen = raf.length();
        long pos = 0;
        raf.seek(0);
        while (pos < fileLen) {
            if (fileLen - pos < FRAME_HEADER_SIZE) break;
            int len = raf.readInt();
            int expectedCrc = raf.readInt();
            if (len <= 0 || pos + FRAME_HEADER_SIZE + len > fileLen) {
                raf.setLength(pos);
                break;
            }
            byte[] buf = new byte[len];
            raf.readFully(buf);
            if (crc32(buf) != expectedCrc) {
                raf.setLength(pos);
                break;
            }
            try {
                LogEntry entry = LogEntry.parseFrom(buf);
                long snapshotIdx = snapshotMeta != null ? snapshotMeta.lastIncludedIndex : 0;
                if (entry.getIndex() > snapshotIdx) {
                    index.put(entry.getIndex(), new WalEntry(segId, pos + FRAME_HEADER_SIZE, len));
                }
                lastLogIndex = entry.getIndex();
                lastLogTerm = entry.getTerm();
            } catch (InvalidProtocolBufferException e) {
                raf.setLength(pos);
                break;
            }
            pos += FRAME_HEADER_SIZE + len;
        }
    }

    @Override
    public synchronized void close() {
        syncExecutor.shutdown();
        try {
            activeSegment.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
