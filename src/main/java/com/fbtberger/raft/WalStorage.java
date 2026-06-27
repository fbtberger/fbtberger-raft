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
import java.nio.file.Files;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * {@link RaftStorage} backed by an append-only Write-Ahead Log file.
 *
 * <p>Layout on disk:
 * <ul>
 *   <li>{@code wal.log} — append-only sequence of length-prefixed protobuf
 *       {@link LogEntry} records: {@code [4B length][protobuf bytes]...}</li>
 *   <li>{@code meta} — currentTerm (8B) + votedFor length (4B) + votedFor
 *       UTF-8 bytes; atomically replaced on each update via rename</li>
 *   <li>{@code snapshot} — snapshot metadata + payload; atomically
 *       replaced via rename</li>
 * </ul>
 *
 * <p>On startup, the WAL file is scanned sequentially to rebuild the
 * in-memory index (log index → file offset). This is intentionally simple:
 * no checkpointing or segment rotation, suitable for the same demo/learning
 * scope as the rest of this project.
 */
public final class WalStorage implements RaftStorage {

    private final File dataDir;
    private final File walFile;
    private final File metaFile;
    private final File snapshotFile;
    private final ExecutorService syncExecutor;

    private RandomAccessFile wal;
    private final NavigableMap<Long, WalEntry> index = new TreeMap<>();

    private long currentTerm;
    private String votedFor;
    private Snapshot snapshot;
    private long lastLogIndex;
    private long lastLogTerm;

    private record WalEntry(long fileOffset, int length) {}

    public WalStorage(File dataDir) {
        this.dataDir = dataDir;
        this.walFile = new File(dataDir, "wal.log");
        this.metaFile = new File(dataDir, "meta");
        this.snapshotFile = new File(dataDir, "snapshot");
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
            this.wal = new RandomAccessFile(walFile, "rw");
            recoverWal();
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
            wal.seek(we.fileOffset);
            byte[] buf = new byte[we.length];
            wal.readFully(buf);
            return LogEntry.parseFrom(buf);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized long getLastLogIndex() {
        return lastLogIndex;
    }

    @Override
    public synchronized long getLastLogTerm() {
        return lastLogTerm;
    }

    @Override
    public synchronized void appendEntries(Iterable<LogEntry> entries) {
        appendToWal(entries);
        syncWal();
    }

    @Override
    public synchronized CompletableFuture<Void> appendEntriesDeferSync(Iterable<LogEntry> entries) {
        appendToWal(entries);
        return CompletableFuture.runAsync(this::syncWal, syncExecutor);
    }

    private void appendToWal(Iterable<LogEntry> entries) {
        try {
            long pos = wal.length();
            wal.seek(pos);
            for (LogEntry entry : entries) {
                byte[] bytes = entry.toByteArray();
                long entryOffset = wal.getFilePointer();
                wal.writeInt(bytes.length);
                wal.write(bytes);
                index.put(entry.getIndex(), new WalEntry(entryOffset + 4, bytes.length));
                lastLogIndex = entry.getIndex();
                lastLogTerm = entry.getTerm();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private synchronized void syncWal() {
        try {
            wal.getFD().sync();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void truncateFrom(long fromIndexInclusive) {
        NavigableMap<Long, WalEntry> tail = index.tailMap(fromIndexInclusive, true);
        if (tail.isEmpty()) return;

        WalEntry first = tail.firstEntry().getValue();
        long truncateOffset = first.fileOffset - 4;
        tail.clear();

        try {
            wal.setLength(truncateOffset);
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

        // Remove compacted entries from index and rewrite WAL without them
        index.headMap(snap.lastIncludedIndex, true).clear();

        if (lastLogIndex <= snap.lastIncludedIndex) {
            lastLogIndex = snap.lastIncludedIndex;
            lastLogTerm = snap.lastIncludedTerm;
        }

        compactWalFile();
    }

    private void compactWalFile() {
        try {
            if (index.isEmpty()) {
                wal.setLength(0);
                return;
            }
            long firstOffset = index.firstEntry().getValue().fileOffset - 4;
            if (firstOffset <= 0) return;

            File tmp = new File(dataDir, "wal.tmp");
            try (RandomAccessFile src = new RandomAccessFile(walFile, "r");
                 FileOutputStream dst = new FileOutputStream(tmp)) {
                src.seek(firstOffset);
                byte[] buf = new byte[8192];
                int read;
                while ((read = src.read(buf)) > 0) {
                    dst.write(buf, 0, read);
                }
                dst.getFD().sync();
            }

            wal.close();
            if (!tmp.renameTo(walFile)) {
                throw new IOException("rename wal.tmp -> wal.log failed");
            }
            wal = new RandomAccessFile(walFile, "rw");

            // Rebuild offsets after compaction
            index.clear();
            recoverWal();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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

    private void recoverWal() throws IOException {
        long fileLen = wal.length();
        long pos = 0;
        wal.seek(0);
        while (pos < fileLen) {
            if (fileLen - pos < 4) break;
            int len = wal.readInt();
            if (len <= 0 || pos + 4 + len > fileLen) {
                wal.setLength(pos);
                break;
            }
            byte[] buf = new byte[len];
            wal.readFully(buf);
            try {
                LogEntry entry = LogEntry.parseFrom(buf);
                index.put(entry.getIndex(), new WalEntry(pos + 4, len));
                lastLogIndex = entry.getIndex();
                lastLogTerm = entry.getTerm();
            } catch (InvalidProtocolBufferException e) {
                wal.setLength(pos);
                break;
            }
            pos += 4 + len;
        }
    }

    @Override
    public synchronized void close() {
        syncExecutor.shutdown();
        try {
            wal.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
