package com.kvstore.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An isolated partition (Shard) of the key-value store protected by a fine-grained
 * ReadWriteLock. Maintains an in-memory map and a doubly-linked LRU chain.
 */
public final class Shard {
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    private final Map<String, Entry> map = new HashMap<>();

    // Doubly linked list for LRU eviction:
    // head = Most Recently Used (MRU), tail = Least Recently Used (LRU)
    private Entry head;
    private Entry tail;

    private long totalMemoryBytes = 0;

    public Shard() {
    }

    /**
     * Retrieves the value for the given key.
     * Evaluates passive expiration and touches LRU position on access.
     */
    public byte[] get(String key) {
        readLock.lock();
        Entry entry = map.get(key);
        if (entry == null) {
            readLock.unlock();
            return null;
        }

        // Check passive expiration
        if (entry.isExpired()) {
            readLock.unlock();
            // Upgrade to write lock to remove expired entry
            writeLock.lock();
            try {
                Entry current = map.get(key);
                if (current != null && current.isExpired()) {
                    removeEntry(current);
                }
                return null;
            } finally {
                writeLock.unlock();
            }
        }

        readLock.unlock();

        // Update LRU position under write lock
        writeLock.lock();
        try {
            Entry current = map.get(key);
            if (current != null && !current.isExpired()) {
                current.touch();
                moveToHead(current);
                return current.getValue();
            }
            return null;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Checks if key exists and is not expired.
     */
    public boolean exists(String key) {
        readLock.lock();
        try {
            Entry entry = map.get(key);
            if (entry == null) {
                return false;
            }
            if (entry.isExpired()) {
                // Expired, will be cleaned up passively or actively
                return false;
            }
            return true;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Sets a key-value pair with an optional expiration timestamp.
     * Inserts into map, adjusts memory metrics, and links at the MRU head.
     */
    public void set(String key, byte[] value, long expireAtMillis) {
        writeLock.lock();
        try {
            Entry existing = map.get(key);
            if (existing != null) {
                totalMemoryBytes -= existing.getEstimatedMemoryBytes();
                existing.setValue(value);
                existing.setExpireAtMillis(expireAtMillis);
                moveToHead(existing);
                totalMemoryBytes += existing.getEstimatedMemoryBytes();
            } else {
                Entry newEntry = new Entry(key, value, expireAtMillis);
                map.put(key, newEntry);
                addToHead(newEntry);
                totalMemoryBytes += newEntry.getEstimatedMemoryBytes();
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Deletes a key from this shard.
     * Returns true if key was present and deleted.
     */
    public boolean del(String key) {
        writeLock.lock();
        try {
            Entry entry = map.get(key);
            if (entry != null) {
                removeEntry(entry);
                return true;
            }
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Sets expiration timestamp (in absolute epoch milliseconds).
     */
    public boolean expireAt(String key, long expireAtMillis) {
        writeLock.lock();
        try {
            Entry entry = map.get(key);
            if (entry == null || entry.isExpired()) {
                if (entry != null) {
                    removeEntry(entry);
                }
                return false;
            }
            entry.setExpireAtMillis(expireAtMillis);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns remaining TTL in seconds.
     * -2: Key does not exist or is expired.
     * -1: Key exists but has no associated expire.
     * >=0: Remaining TTL in seconds.
     */
    public long ttl(String key) {
        long pttl = pttl(key);
        if (pttl < 0) {
            return pttl;
        }
        return (pttl + 999) / 1000L; // Round up like Redis
    }

    /**
     * Returns remaining TTL in milliseconds.
     */
    public long pttl(String key) {
        readLock.lock();
        try {
            Entry entry = map.get(key);
            if (entry == null) {
                return -2;
            }
            long expireAt = entry.getExpireAtMillis();
            if (expireAt <= 0) {
                return -1;
            }
            long now = System.currentTimeMillis();
            if (now >= expireAt) {
                return -2;
            }
            return expireAt - now;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Evicts the least recently used (tail) element.
     * Returns the evicted key, or null if the shard is empty.
     */
    public String evictOldest() {
        writeLock.lock();
        try {
            if (tail == null) {
                return null;
            }
            String evictedKey = tail.getKey();
            removeEntry(tail);
            return evictedKey;
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Actively scans up to sampleCount entries and evicts expired keys.
     * Returns the number of keys evicted in this cycle.
     */
    public int cleanExpired(int sampleCount) {
        int cleaned = 0;
        long now = System.currentTimeMillis();
        List<Entry> toRemove = new ArrayList<>();

        writeLock.lock();
        try {
            int checked = 0;
            Iterator<Entry> it = map.values().iterator();
            while (it.hasNext() && checked < sampleCount) {
                Entry entry = it.next();
                checked++;
                if (entry.isExpiredAt(now)) {
                    toRemove.add(entry);
                }
            }

            for (Entry entry : toRemove) {
                removeEntry(entry);
                cleaned++;
            }
        } finally {
            writeLock.unlock();
        }
        return cleaned;
    }

    /**
     * Retrieves all non-expired keys matching a simple wildcard glob.
     */
    public List<String> getMatchingKeys(String globPattern) {
        List<String> results = new ArrayList<>();
        long now = System.currentTimeMillis();
        readLock.lock();
        try {
            for (Entry entry : map.values()) {
                if (!entry.isExpiredAt(now)) {
                    if (globPattern.equals("*") || matchPattern(entry.getKey(), globPattern)) {
                        results.add(entry.getKey());
                    }
                }
            }
        } finally {
            readLock.unlock();
        }
        return results;
    }

    public int size() {
        readLock.lock();
        try {
            return map.size();
        } finally {
            readLock.unlock();
        }
    }

    public long getMemoryBytes() {
        readLock.lock();
        try {
            return totalMemoryBytes;
        } finally {
            readLock.unlock();
        }
    }

    public void flush() {
        writeLock.lock();
        try {
            map.clear();
            head = null;
            tail = null;
            totalMemoryBytes = 0;
        } finally {
            writeLock.unlock();
        }
    }

    // --- LRU Doubly-Linked List Internal Helpers (Must be called under writeLock) ---

    private void addToHead(Entry entry) {
        entry.next = head;
        entry.prev = null;
        if (head != null) {
            head.prev = entry;
        }
        head = entry;
        if (tail == null) {
            tail = entry;
        }
    }

    private void moveToHead(Entry entry) {
        if (entry == head) {
            return;
        }
        unlink(entry);
        addToHead(entry);
    }

    private void unlink(Entry entry) {
        if (entry.prev != null) {
            entry.prev.next = entry.next;
        } else {
            head = entry.next;
        }

        if (entry.next != null) {
            entry.next.prev = entry.prev;
        } else {
            tail = entry.prev;
        }

        entry.prev = null;
        entry.next = null;
    }

    private void removeEntry(Entry entry) {
        map.remove(entry.getKey());
        unlink(entry);
        totalMemoryBytes -= entry.getEstimatedMemoryBytes();
        if (totalMemoryBytes < 0) {
            totalMemoryBytes = 0;
        }
    }

    private boolean matchPattern(String text, String pattern) {
        // Simple regex-based glob matcher
        String regex = "^" + pattern.replace(".", "\\.").replace("*", ".*").replace("?", ".") + "$";
        return text.matches(regex);
    }
}
