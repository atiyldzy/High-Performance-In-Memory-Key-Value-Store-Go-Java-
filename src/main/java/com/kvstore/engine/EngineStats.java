package com.kvstore.engine;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Real-time operational metrics and counters for the storage engine.
 */
public class EngineStats {
    private final AtomicLong totalCommandsProcessed = new AtomicLong(0);
    private final AtomicLong getHits = new AtomicLong(0);
    private final AtomicLong getMisses = new AtomicLong(0);
    private final AtomicLong totalSetOps = new AtomicLong(0);
    private final AtomicLong totalDelOps = new AtomicLong(0);
    private final AtomicLong totalEvictedKeys = new AtomicLong(0);
    private final AtomicLong totalExpiredKeys = new AtomicLong(0);
    private final long startTimeMillis = System.currentTimeMillis();

    public void incrementCommands() {
        totalCommandsProcessed.incrementAndGet();
    }

    public void recordHit() {
        getHits.incrementAndGet();
    }

    public void recordMiss() {
        getMisses.incrementAndGet();
    }

    public void recordSet() {
        totalSetOps.incrementAndGet();
    }

    public void recordDel() {
        totalDelOps.incrementAndGet();
    }

    public void recordEviction() {
        totalEvictedKeys.incrementAndGet();
    }

    public void recordExpired(long count) {
        totalExpiredKeys.addAndGet(count);
    }

    public long getTotalCommandsProcessed() {
        return totalCommandsProcessed.get();
    }

    public long getGetHits() {
        return getHits.get();
    }

    public long getGetMisses() {
        return getMisses.get();
    }

    public long getTotalSetOps() {
        return totalSetOps.get();
    }

    public long getTotalDelOps() {
        return totalDelOps.get();
    }

    public long getTotalEvictedKeys() {
        return totalEvictedKeys.get();
    }

    public long getTotalExpiredKeys() {
        return totalExpiredKeys.get();
    }

    public double getHitRate() {
        long hits = getHits.get();
        long total = hits + getMisses.get();
        return total == 0 ? 0.0 : ((double) hits / total) * 100.0;
    }

    public long getUptimeSeconds() {
        return (System.currentTimeMillis() - startTimeMillis) / 1000L;
    }
}
