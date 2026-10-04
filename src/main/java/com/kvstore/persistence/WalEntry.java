package com.kvstore.persistence;

import com.kvstore.protocol.RespFormatter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * Representation of a state-mutating command logged into the Write-Ahead Log (AOF).
 */
public final class WalEntry {
    public enum Type {
        SET,
        DEL,
        EXPIREAT,
        FLUSHDB
    }

    private final Type type;
    private final String key;
    private final byte[] value;
    private final long expireAtMillis;

    public WalEntry(Type type, String key, byte[] value, long expireAtMillis) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.expireAtMillis = expireAtMillis;
    }

    public static WalEntry set(String key, byte[] value, long expireAtMillis) {
        return new WalEntry(Type.SET, key, value, expireAtMillis);
    }

    public static WalEntry del(String key) {
        return new WalEntry(Type.DEL, key, null, -1);
    }

    public static WalEntry expireAt(String key, long expireAtMillis) {
        return new WalEntry(Type.EXPIREAT, key, null, expireAtMillis);
    }

    public static WalEntry flushDb() {
        return new WalEntry(Type.FLUSHDB, null, null, -1);
    }

    public Type getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getExpireAtMillis() {
        return expireAtMillis;
    }

    /**
     * Serializes this mutation into standard Redis-compatible RESP format for AOF append.
     */
    public byte[] toRespBytes() {
        switch (type) {
            case SET -> {
                if (expireAtMillis > 0) {
                    return RespFormatter.array(List.of(
                            "SET".getBytes(StandardCharsets.UTF_8),
                            key.getBytes(StandardCharsets.UTF_8),
                            value,
                            "PXAT".getBytes(StandardCharsets.UTF_8),
                            String.valueOf(expireAtMillis).getBytes(StandardCharsets.UTF_8)
                    ));
                } else {
                    return RespFormatter.array(List.of(
                            "SET".getBytes(StandardCharsets.UTF_8),
                            key.getBytes(StandardCharsets.UTF_8),
                            value
                    ));
                }
            }
            case DEL -> {
                return RespFormatter.array(List.of(
                        "DEL".getBytes(StandardCharsets.UTF_8),
                        key.getBytes(StandardCharsets.UTF_8)
                ));
            }
            case EXPIREAT -> {
                return RespFormatter.array(List.of(
                        "PEXPIREAT".getBytes(StandardCharsets.UTF_8),
                        key.getBytes(StandardCharsets.UTF_8),
                        String.valueOf(expireAtMillis).getBytes(StandardCharsets.UTF_8)
                ));
            }
            case FLUSHDB -> {
                return RespFormatter.array(List.of(
                        "FLUSHDB".getBytes(StandardCharsets.UTF_8)
                ));
            }
            default -> throw new IllegalStateException("Unknown type: " + type);
        }
    }
}
