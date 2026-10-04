package com.kvstore.engine;

import com.kvstore.config.ServerConfig;
import java.util.List;

/**
 * Contract defining high-performance Key-Value storage engine capabilities.
 */
public interface DataStore extends AutoCloseable {

    /**
     * Retrieves value for key or null if absent/expired.
     */
    byte[] get(String key);

    /**
     * Stores key-value with optional absolute expiry epoch timestamp (-1 for no expiry).
     */
    boolean set(String key, byte[] value, long expireAtMillis);

    /**
     * Stores string key-value with no expiration.
     */
    default boolean set(String key, String value) {
        return set(key, value.getBytes(java.nio.charset.StandardCharsets.UTF_8), -1);
    }

    /**
     * Stores string key-value with TTL in seconds.
     */
    default boolean setEx(String key, byte[] value, long seconds) {
        long expireAt = seconds > 0 ? System.currentTimeMillis() + (seconds * 1000L) : -1;
        return set(key, value, expireAt);
    }

    /**
     * Stores string key-value with TTL in milliseconds.
     */
    default boolean setPx(String key, byte[] value, long millis) {
        long expireAt = millis > 0 ? System.currentTimeMillis() + millis : -1;
        return set(key, value, expireAt);
    }

    /**
     * Deletes one or more keys. Returns number of keys removed.
     */
    long del(String... keys);

    /**
     * Sets TTL on key in seconds. Returns true if key exists and expiration was set.
     */
    boolean expire(String key, long seconds);

    /**
     * Sets TTL on key in milliseconds.
     */
    boolean pexpire(String key, long millis);

    /**
     * Sets absolute expiration timestamp in epoch milliseconds.
     */
    boolean expireAt(String key, long expireAtMillis);

    /**
     * Returns TTL in seconds (-2 if absent/expired, -1 if immortal, >=0 remaining seconds).
     */
    long ttl(String key);

    /**
     * Returns TTL in milliseconds (-2 if absent/expired, -1 if immortal, >=0 remaining millis).
     */
    long pttl(String key);

    /**
     * Checks if key exists.
     */
    boolean exists(String key);

    /**
     * Returns matching keys based on glob pattern.
     */
    List<String> keys(String pattern);

    /**
     * Returns total number of keys stored across all shards.
     */
    long dbsize();

    /**
     * Total memory used across all shards in bytes.
     */
    long memoryUsage();

    /**
     * Clears all data in the store.
     */
    void flushDb();

    /**
     * Returns operational telemetry metrics.
     */
    EngineStats getStats();

    /**
     * Returns server configuration.
     */
    ServerConfig getConfig();

    @Override
    void close();
}
