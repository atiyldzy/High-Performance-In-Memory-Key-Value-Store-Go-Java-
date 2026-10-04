package com.kvstore.engine;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background active expiration daemon inspired by Redis activeExpireCycle.
 * Periodically samples keys across shards to proactively evict expired items
 * without waiting for passive access.
 */
public final class ExpireManager implements AutoCloseable {
    private final ShardedStorageEngine engine;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ExpireManager(ShardedStorageEngine engine) {
        this.engine = engine;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kv-active-expire-worker");
            t.setDaemon(true);
            return t;
        });
    }

    public void start(int intervalMs, int sampleCountPerShard) {
        if (running.compareAndSet(false, true)) {
            scheduler.scheduleWithFixedDelay(() -> {
                try {
                    runActiveCycle(sampleCountPerShard);
                } catch (Throwable t) {
                    // Suppress exceptions from breaking schedule
                }
            }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        }
    }

    private void runActiveCycle(int sampleCount) {
        if (!running.get()) {
            return;
        }
        Shard[] shards = engine.getShards();
        long totalCleaned = 0;
        for (Shard shard : shards) {
            int cleaned = shard.cleanExpired(sampleCount);
            totalCleaned += cleaned;
        }
        if (totalCleaned > 0) {
            engine.getStats().recordExpired(totalCleaned);
        }
    }

    @Override
    public void close() {
        if (running.compareAndSet(true, false)) {
            scheduler.shutdownNow();
        }
    }
}
