package com.kvstore.persistence;

import com.kvstore.config.ServerConfig;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * High-performance Asynchronous Append-Only File (AOF) / Write-Ahead Logging (WAL) engine.
 * Mutating commands are dispatched via a lock-free/low-contention queue and flushed in
 * batches by a dedicated background I/O worker thread with configurable fsync policies.
 */
public final class AofManager implements AutoCloseable {
    private static final int QUEUE_CAPACITY = 65536;
    private static final int BATCH_SIZE = 1024;

    private final ServerConfig config;
    private final BlockingQueue<WalEntry> queue;
    private final AtomicBoolean running;
    private final Thread aofWriterThread;

    private FileOutputStream fileOutputStream;
    private FileChannel fileChannel;
    private long lastFsyncTimestamp;

    public AofManager(ServerConfig config) {
        this.config = config;
        this.queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        this.running = new AtomicBoolean(false);
        this.lastFsyncTimestamp = System.currentTimeMillis();

        this.aofWriterThread = new Thread(this::runWriterLoop, "kv-aof-writer");
        this.aofWriterThread.setDaemon(true);
    }

    public synchronized void start() throws IOException {
        if (!config.isAofEnabled()) {
            return;
        }

        File file = new File(config.getAofFilePath());
        if (file.getParentFile() != null && !file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }

        // Open in append mode
        this.fileOutputStream = new FileOutputStream(file, true);
        this.fileChannel = fileOutputStream.getChannel();

        if (running.compareAndSet(false, true)) {
            aofWriterThread.start();
        }
    }

    /**
     * Non-blocking or bounded offering of a WAL mutation to the persistence queue.
     */
    public boolean append(WalEntry entry) {
        if (!config.isAofEnabled() || !running.get()) {
            return true;
        }
        return queue.offer(entry);
    }

    /**
     * Dedicated background I/O loop that drains batches of mutations and syncs to disk.
     */
    private void runWriterLoop() {
        List<WalEntry> batch = new ArrayList<>(BATCH_SIZE);

        while (running.get() || !queue.isEmpty()) {
            try {
                // Wait for at least one entry
                WalEntry first = queue.poll(100, TimeUnit.MILLISECONDS);
                if (first != null) {
                    batch.add(first);
                    queue.drainTo(batch, BATCH_SIZE - 1);

                    // Compute total byte size for the batch
                    int totalBytes = 0;
                    List<byte[]> serializedEntries = new ArrayList<>(batch.size());
                    for (WalEntry item : batch) {
                        byte[] bytes = item.toRespBytes();
                        serializedEntries.add(bytes);
                        totalBytes += bytes.length;
                    }

                    // Direct ByteBuffer batch write to FileChannel
                    ByteBuffer buffer = ByteBuffer.allocateDirect(totalBytes);
                    for (byte[] bytes : serializedEntries) {
                        buffer.put(bytes);
                    }
                    buffer.flip();

                    while (buffer.hasRemaining()) {
                        fileChannel.write(buffer);
                    }

                    // Apply Fsync Policy
                    applyFsyncPolicy();
                    batch.clear();
                } else {
                    // Periodic fsync check during idle periods
                    applyPeriodicFsync();
                }
            } catch (InterruptedException e) {
                // Thread interrupted on shutdown, flush remaining
                break;
            } catch (IOException e) {
                System.err.println("[AOF-ERROR] Write error in AOF background thread: " + e.getMessage());
            }
        }

        // Final flush before complete shutdown
        flushRemaining();
    }

    private void applyFsyncPolicy() throws IOException {
        AofSyncPolicy policy = config.getAofSyncPolicy();
        if (policy == AofSyncPolicy.ALWAYS) {
            fileChannel.force(false);
            lastFsyncTimestamp = System.currentTimeMillis();
        } else if (policy == AofSyncPolicy.EVERYSEC) {
            long now = System.currentTimeMillis();
            if (now - lastFsyncTimestamp >= 1000L) {
                fileChannel.force(false);
                lastFsyncTimestamp = now;
            }
        }
    }

    private void applyPeriodicFsync() throws IOException {
        if (config.getAofSyncPolicy() == AofSyncPolicy.EVERYSEC) {
            long now = System.currentTimeMillis();
            if (now - lastFsyncTimestamp >= 1000L) {
                fileChannel.force(false);
                lastFsyncTimestamp = now;
            }
        }
    }

    private void flushRemaining() {
        try {
            List<WalEntry> remaining = new ArrayList<>();
            queue.drainTo(remaining);
            if (!remaining.isEmpty()) {
                int totalBytes = 0;
                List<byte[]> serializedEntries = new ArrayList<>(remaining.size());
                for (WalEntry item : remaining) {
                    byte[] bytes = item.toRespBytes();
                    serializedEntries.add(bytes);
                    totalBytes += bytes.length;
                }
                ByteBuffer buffer = ByteBuffer.allocateDirect(totalBytes);
                for (byte[] bytes : serializedEntries) {
                    buffer.put(bytes);
                }
                buffer.flip();
                while (buffer.hasRemaining()) {
                    fileChannel.write(buffer);
                }
            }
            if (fileChannel != null && fileChannel.isOpen()) {
                fileChannel.force(true);
            }
        } catch (IOException ignored) {
        }
    }

    @Override
    public synchronized void close() {
        if (running.compareAndSet(true, false)) {
            aofWriterThread.interrupt();
            try {
                aofWriterThread.join(2000);
            } catch (InterruptedException ignored) {
            }

            try {
                if (fileChannel != null && fileChannel.isOpen()) {
                    fileChannel.close();
                }
                if (fileOutputStream != null) {
                    fileOutputStream.close();
                }
            } catch (IOException ignored) {
            }
        }
    }
}
