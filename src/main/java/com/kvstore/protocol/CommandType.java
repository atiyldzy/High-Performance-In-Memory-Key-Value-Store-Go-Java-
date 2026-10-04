package com.kvstore.protocol;

/**
 * Enumeration of supported database commands.
 */
public enum CommandType {
    GET,
    SET,
    DEL,
    EXPIRE,
    PEXPIRE,
    EXPIREAT,
    PEXPIREAT,
    TTL,
    PTTL,
    EXISTS,
    KEYS,
    DBSIZE,
    FLUSHDB,
    PING,
    INFO,
    COMMAND, // Handshake/compat for redis-cli
    QUIT,
    SHUTDOWN,
    UNKNOWN;

    public static CommandType fromString(String name) {
        if (name == null || name.isEmpty()) {
            return UNKNOWN;
        }
        try {
            return CommandType.valueOf(name.toUpperCase());
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
