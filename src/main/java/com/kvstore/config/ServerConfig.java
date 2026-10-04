package com.kvstore.config;

import com.kvstore.engine.EvictionPolicy;
import com.kvstore.persistence.AofSyncPolicy;

/**
 * Global Server Configuration parameters.
 */
public class ServerConfig {
    private String host = "0.0.0.0";
    private int port = 6379;
    private int shardCount = 64;
    private long maxKeys = 1_000_000L;
    private long maxMemoryBytes = 512 * 1024 * 1024L; // 512MB default
    private EvictionPolicy evictionPolicy = EvictionPolicy.LRU;

    // Persistence settings
    private boolean aofEnabled = true;
    private String aofFilePath = "appendonly.aof";
    private AofSyncPolicy aofSyncPolicy = AofSyncPolicy.EVERYSEC;

    // Active expiration settings
    private boolean activeExpirationEnabled = true;
    private int activeExpireIntervalMs = 100;
    private int activeExpireSampleCount = 20;

    public ServerConfig() {
    }

    public static ServerConfig createDefault() {
        return new ServerConfig();
    }

    public String getHost() {
        return host;
    }

    public ServerConfig setHost(String host) {
        this.host = host;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ServerConfig setPort(int port) {
        this.port = port;
        return this;
    }

    public int getShardCount() {
        return shardCount;
    }

    public ServerConfig setShardCount(int shardCount) {
        this.shardCount = shardCount;
        return this;
    }

    public long getMaxKeys() {
        return maxKeys;
    }

    public ServerConfig setMaxKeys(long maxKeys) {
        this.maxKeys = maxKeys;
        return this;
    }

    public long getMaxMemoryBytes() {
        return maxMemoryBytes;
    }

    public ServerConfig setMaxMemoryBytes(long maxMemoryBytes) {
        this.maxMemoryBytes = maxMemoryBytes;
        return this;
    }

    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    public ServerConfig setEvictionPolicy(EvictionPolicy evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
        return this;
    }

    public boolean isAofEnabled() {
        return aofEnabled;
    }

    public ServerConfig setAofEnabled(boolean aofEnabled) {
        this.aofEnabled = aofEnabled;
        return this;
    }

    public String getAofFilePath() {
        return aofFilePath;
    }

    public ServerConfig setAofFilePath(String aofFilePath) {
        this.aofFilePath = aofFilePath;
        return this;
    }

    public AofSyncPolicy getAofSyncPolicy() {
        return aofSyncPolicy;
    }

    public ServerConfig setAofSyncPolicy(AofSyncPolicy aofSyncPolicy) {
        this.aofSyncPolicy = aofSyncPolicy;
        return this;
    }

    public boolean isActiveExpirationEnabled() {
        return activeExpirationEnabled;
    }

    public ServerConfig setActiveExpirationEnabled(boolean activeExpirationEnabled) {
        this.activeExpirationEnabled = activeExpirationEnabled;
        return this;
    }

    public int getActiveExpireIntervalMs() {
        return activeExpireIntervalMs;
    }

    public ServerConfig setActiveExpireIntervalMs(int activeExpireIntervalMs) {
        this.activeExpireIntervalMs = activeExpireIntervalMs;
        return this;
    }

    public int getActiveExpireSampleCount() {
        return activeExpireSampleCount;
    }

    public ServerConfig setActiveExpireSampleCount(int activeExpireSampleCount) {
        this.activeExpireSampleCount = activeExpireSampleCount;
        return this;
    }
}
