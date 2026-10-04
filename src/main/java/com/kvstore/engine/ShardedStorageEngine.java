package com.kvstore.engine;

import com.kvstore.config.ServerConfig;
import com.kvstore.persistence.AofManager;
import com.kvstore.persistence.WalEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * High-performance, sharded, thread-safe in-memory key-value database engine.
 * Sharding divides the key space across independent locks to maximize concurrency.
 */
public final class ShardedStorageEngine implements DataStore {
    private final ServerConfig config;
    private final Shard[] shards;
    private final int shardCount;
    private final EngineStats stats;
    private final ExpireManager expireManager;
    private final AofManager aofManager;

    public ShardedStorageEngine(ServerConfig config) {
        this.config = config;
        this.shardCount = Math.max(1, config.getShardCount());
        this.shards = new Shard[shardCount];
        for (int i = 0; i < shardCount; i++) {
            this.shards[i] = new Shard();
        }
        this.stats = new EngineStats();
        this.expireManager = new ExpireManager(this);
        this.aofManager = new AofManager(config);
    }

    public void init() throws IOException {
        if (config.isAofEnabled()) {
            aofManager.start();
        }
        if (config.isActiveExpirationEnabled()) {
            expireManager.start(config.getActiveExpireIntervalMs(), config.getActiveExpireSampleCount());
        }
    }

    private Shard getShard(String key) {
        int hash = key.hashCode();
        int index = (hash & 0x7FFFFFFF) % shardCount;
        return shards[index];
    }

    @Override
    public byte[] get(String key) {
        if (key == null) return null;
        stats.incrementCommands();

        Shard shard = getShard(key);
        byte[] val = shard.get(key);
        if (val != null) {
            stats.recordHit();
        } else {
            stats.recordMiss();
        }
        return val;
    }

    @Override
    public boolean set(String key, byte[] value, long expireAtMillis) {
        if (key == null || value == null) return false;
        stats.incrementCommands();

        // Eviction check before insertion
        checkAndEvictIfNecessary();

        Shard shard = getShard(key);
        shard.set(key, value, expireAtMillis);
        stats.recordSet();

        // Write-Ahead Logging
        aofManager.append(WalEntry.set(key, value, expireAtMillis));
        return true;
    }

    @Override
    public long del(String... keys) {
        if (keys == null || keys.length == 0) return 0;
        stats.incrementCommands();

        long deleted = 0;
        for (String key : keys) {
            if (key != null) {
                Shard shard = getShard(key);
                if (shard.del(key)) {
                    deleted++;
                    stats.recordDel();
                    aofManager.append(WalEntry.del(key));
                }
            }
        }
        return deleted;
    }

    @Override
    public boolean expire(String key, long seconds) {
        if (key == null) return false;
        long expireAt = seconds > 0 ? System.currentTimeMillis() + (seconds * 1000L) : -1;
        return expireAt(key, expireAt);
    }

    @Override
    public boolean pexpire(String key, long millis) {
        if (key == null) return false;
        long expireAt = millis > 0 ? System.currentTimeMillis() + millis : -1;
        return expireAt(key, expireAt);
    }

    @Override
    public boolean expireAt(String key, long expireAtMillis) {
        if (key == null) return false;
        stats.incrementCommands();

        Shard shard = getShard(key);
        boolean result = shard.expireAt(key, expireAtMillis);
        if (result) {
            aofManager.append(WalEntry.expireAt(key, expireAtMillis));
        }
        return result;
    }

    @Override
    public long ttl(String key) {
        if (key == null) return -2;
        stats.incrementCommands();
        Shard shard = getShard(key);
        return shard.ttl(key);
    }

    @Override
    public long pttl(String key) {
        if (key == null) return -2;
        stats.incrementCommands();
        Shard shard = getShard(key);
        return shard.pttl(key);
    }

    @Override
    public boolean exists(String key) {
        if (key == null) return false;
        stats.incrementCommands();
        Shard shard = getShard(key);
        return shard.exists(key);
    }

    @Override
    public List<String> keys(String pattern) {
        stats.incrementCommands();
        List<String> allKeys = new ArrayList<>();
        String glob = (pattern == null || pattern.isEmpty()) ? "*" : pattern;
        for (Shard shard : shards) {
            allKeys.addAll(shard.getMatchingKeys(glob));
        }
        return allKeys;
    }

    @Override
    public long dbsize() {
        stats.incrementCommands();
        long total = 0;
        for (Shard shard : shards) {
            total += shard.size();
        }
        return total;
    }

    @Override
    public long memoryUsage() {
        long total = 0;
        for (Shard shard : shards) {
            total += shard.getMemoryBytes();
        }
        return total;
    }

    @Override
    public void flushDb() {
        stats.incrementCommands();
        for (Shard shard : shards) {
            shard.flush();
        }
        aofManager.append(WalEntry.flushDb());
    }

    @Override
    public EngineStats getStats() {
        return stats;
    }

    @Override
    public ServerConfig getConfig() {
        return config;
    }

    public Shard[] getShards() {
        return shards;
    }

    /**
     * Proactive LRU Eviction check when total keys or total estimated memory exceeds limit.
     */
    private void checkAndEvictIfNecessary() {
        long maxKeys = config.getMaxKeys();
        long maxMemory = config.getMaxMemoryBytes();
        if (maxKeys <= 0 && maxMemory <= 0) {
            return;
        }

        if (config.getEvictionPolicy() == EvictionPolicy.LRU) {
            // Check if capacity/memory exceeded
            while ((maxKeys > 0 && dbsize() >= maxKeys) || (maxMemory > 0 && memoryUsage() >= maxMemory)) {
                // Sample random shards and evict the oldest entry
                boolean evictedAny = false;
                int startIdx = ThreadLocalRandom.current().nextInt(shardCount);
                for (int i = 0; i < shardCount; i++) {
                    int idx = (startIdx + i) % shardCount;
                    String evictedKey = shards[idx].evictOldest();
                    if (evictedKey != null) {
                        stats.recordEviction();
                        aofManager.append(WalEntry.del(evictedKey));
                        evictedAny = true;
                        break;
                    }
                }
                if (!evictedAny) {
                    break;
                }
            }
        }
    }

    @Override
    public void close() {
        expireManager.close();
        aofManager.close();
    }
}
