package com.kvstore.persistence;

/**
 * Fsync synchronization strategies for the Append-Only File (WAL).
 */
public enum AofSyncPolicy {
    /**
     * fsync on every single write operation (highest durability, lower throughput).
     */
    ALWAYS,

    /**
     * fsync once per second in background thread (balanced durability and performance).
     */
    EVERYSEC,

    /**
     * Do not explicitly fsync; rely on OS page cache flushing (maximum throughput).
     */
    NO
}
