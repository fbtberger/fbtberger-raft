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
    private Snapshot snapshot;
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
            lastLogIndex = snapshot != null ? snapshot.lastIncludedIndex : 0;
            lastLogTerm = snapshot != null ? snapshot.lastIncludedTerm : 0;
        } else {
            var last = index.lastEntry();
            lastLogIndex = last.getKey();
            LogEntry entry = getLogEntry(lastLogIndex);
            lastLogTerm = entry != null ? entry.getTerm() : 0;
        }
    }

    // ---- snapshotting (§7) -----------------------------------------------

    @Override
    public synchronized long getSnapshotIndex() {
        return snapshot != null ? snapshot.lastIncludedIndex : 0;
    }

    @Override
    public synchronized long getSnapshotTerm() {
        return snapshot != null ? snapshot.lastIncludedTerm : 0;
    }

    @Override
    public synchronized Snapshot getSnapshot() { return snapshot; }

    @Override
    public synchronized void saveSnapshotAndCompact(Snapshot snap) {
        // The snapshot boundary must never move backwards: a background (COW)
        // snapshot in RaftNode decides to save off-lock, so if a newer snapshot
        // (e.g. one just installed by InstallSnapshot after a step-down/
        // re-election) landed in between, that stale save is dropped here rather
        // than overwriting the higher boundary and rewinding the compaction.
        if (this.snapshot != null && snap.lastIncludedIndex <= this.snapshot.lastIncludedIndex) {
            return;
        }
        File tmp = new File(dataDir, "snapshot.tmp");
        try (FileOutputStream fos = new FileOutputStream(tmp);
             DataOutputStream dos = new DataOutputStream(fos)) {
            dos.writeLong(snap.lastIncludedIndex);
            dos.writeLong(snap.lastIncludedTerm);
            dos.writeInt(snap.stateMachineData.length);
            dos.write(snap.stateMachineData);
            dos.writeInt(snap.configurationData.length);
            dos.write(snap.configurationData);
            dos.flush();
            fos.getFD().sync();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (!tmp.renameTo(snapshotFile)) {
            throw new UncheckedIOException(new IOException("rename snapshot.tmp -> snapshot failed"));
        }
        this.snapshot = snap;

        index.headMap(snap.lastIncludedIndex, true).clear();

        if (lastLogIndex <= snap.lastIncludedIndex) {
            lastLogIndex = snap.lastIncludedIndex;
            lastLogTerm = snap.lastIncludedTerm;
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
        if (snapshot != null && idx == snapshot.lastIncludedIndex) return snapshot.lastIncludedTerm;
        LogEntry entry = getLogEntry(idx);
        return entry == null ? -1L : entry.getTerm();
    }

    private void recoverSnapshot() throws IOException {
        if (!snapshotFile.exists()) return;
        try (DataInputStream dis = new DataInputStream(new FileInputStream(snapshotFile))) {
            long lastIdx = dis.readLong();
            long lastTerm = dis.readLong();
            int smLen = dis.readInt();
            byte[] smData = dis.readNBytes(smLen);
            int cfgLen = dis.readInt();
            byte[] cfgData = dis.readNBytes(cfgLen);
            this.snapshot = new Snapshot(lastIdx, lastTerm, smData, cfgData);
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
                long snapshotIdx = snapshot != null ? snapshot.lastIncludedIndex : 0;
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
