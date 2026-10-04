package com.kvstore.engine;

/**
 * Supported eviction policies when maximum memory or capacity is reached.
 */
public enum EvictionPolicy {
    /**
     * Least Recently Used: evicts the least recently accessed keys first.
     */
    LRU,

    /**
     * Returns an error on write operations when memory/capacity limit is reached.
     */
    NOEVICTION
}
