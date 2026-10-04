package com.kvstore.engine;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Storage Node representing a key-value record with expiration and LRU tracking pointers.
 */
public final class Entry {
    private final String key;
    private byte[] value;
    private long expireAtMillis; // -1 means never expire
    private long lastAccessTime;

    // Pointers for Shard-specific doubly-linked LRU list
    Entry prev;
    Entry next;

    public Entry(String key, byte[] value, long expireAtMillis) {
        this.key = key;
        this.value = value;
        this.expireAtMillis = expireAtMillis;
        this.lastAccessTime = System.currentTimeMillis();
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
        this.lastAccessTime = System.currentTimeMillis();
    }

    public String getValueAsString() {
        return value != null ? new String(value, StandardCharsets.UTF_8) : null;
    }

    public long getExpireAtMillis() {
        return expireAtMillis;
    }

    public void setExpireAtMillis(long expireAtMillis) {
        this.expireAtMillis = expireAtMillis;
    }

    public boolean isExpired() {
        return expireAtMillis > 0 && System.currentTimeMillis() > expireAtMillis;
    }

    public boolean isExpiredAt(long now) {
        return expireAtMillis > 0 && now > expireAtMillis;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void touch() {
        this.lastAccessTime = System.currentTimeMillis();
    }

    /**
     * Estimates in-memory size of this Entry in bytes.
     * Includes String object overhead, char array, byte array, and node pointer references.
     */
    public long getEstimatedMemoryBytes() {
        long keyBytes = key != null ? (key.length() * 2L + 32) : 0;
        long valBytes = value != null ? (value.length + 24L) : 0;
        long nodeOverhead = 64; // object header + 2 long fields + 4 references
        return keyBytes + valBytes + nodeOverhead;
    }

    @Override
    public String toString() {
        return "Entry{" +
                "key='" + key + '\'' +
                ", expireAtMillis=" + expireAtMillis +
                ", lastAccessTime=" + lastAccessTime +
                '}';
    }
}
