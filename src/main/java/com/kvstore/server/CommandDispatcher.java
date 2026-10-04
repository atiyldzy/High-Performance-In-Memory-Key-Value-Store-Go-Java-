package com.kvstore.server;

import com.kvstore.engine.DataStore;
import com.kvstore.engine.EngineStats;
import com.kvstore.protocol.Command;
import com.kvstore.protocol.CommandType;
import com.kvstore.protocol.RespFormatter;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Routes and executes parsed commands against the DataStore engine.
 */
public final class CommandDispatcher {
    private final DataStore store;

    public CommandDispatcher(DataStore store) {
        this.store = store;
    }

    public byte[] execute(Command command) {
        if (command == null || command.getType() == CommandType.UNKNOWN) {
            return RespFormatter.error("unknown command");
        }

        try {
            return switch (command.getType()) {
                case GET -> handleGet(command);
                case SET -> handleSet(command);
                case DEL -> handleDel(command);
                case EXPIRE -> handleExpire(command);
                case PEXPIRE -> handlePExpire(command);
                case EXPIREAT -> handleExpireAt(command);
                case PEXPIREAT -> handlePExpireAt(command);
                case TTL -> handleTtl(command);
                case PTTL -> handlePttl(command);
                case EXISTS -> handleExists(command);
                case KEYS -> handleKeys(command);
                case DBSIZE -> handleDbSize(command);
                case FLUSHDB -> handleFlushDb(command);
                case PING -> handlePing(command);
                case INFO -> handleInfo(command);
                case COMMAND -> handleCommandCompat(command);
                case QUIT -> RespFormatter.OK_RESP;
                default -> RespFormatter.error("unsupported command: " + command.getType());
            };
        } catch (Exception e) {
            return RespFormatter.error("ERR " + e.getMessage());
        }
    }

    private byte[] handleGet(Command command) {
        if (command.getArgCount() < 1) {
            return RespFormatter.error("wrong number of arguments for 'get' command");
        }
        String key = command.getArgAsString(0);
        byte[] value = store.get(key);
        return RespFormatter.bulkString(value);
    }

    private byte[] handleSet(Command command) {
        if (command.getArgCount() < 2) {
            return RespFormatter.error("wrong number of arguments for 'set' command");
        }
        String key = command.getArgAsString(0);
        byte[] value = command.getArg(1);
        long expireAtMillis = -1;

        // Parse optional TTL flags: EX seconds, PX milliseconds, EXAT timestamp, PXAT millistamp
        int i = 2;
        while (i < command.getArgCount()) {
            String opt = command.getArgAsString(i).toUpperCase();
            if ("EX".equals(opt) && i + 1 < command.getArgCount()) {
                long sec = Long.parseLong(command.getArgAsString(i + 1));
                expireAtMillis = System.currentTimeMillis() + (sec * 1000L);
                i += 2;
            } else if ("PX".equals(opt) && i + 1 < command.getArgCount()) {
                long ms = Long.parseLong(command.getArgAsString(i + 1));
                expireAtMillis = System.currentTimeMillis() + ms;
                i += 2;
            } else if ("EXAT".equals(opt) && i + 1 < command.getArgCount()) {
                long sec = Long.parseLong(command.getArgAsString(i + 1));
                expireAtMillis = sec * 1000L;
                i += 2;
            } else if ("PXAT".equals(opt) && i + 1 < command.getArgCount()) {
                long ms = Long.parseLong(command.getArgAsString(i + 1));
                expireAtMillis = ms;
                i += 2;
            } else {
                i++;
            }
        }

        boolean ok = store.set(key, value, expireAtMillis);
        return ok ? RespFormatter.OK_RESP : RespFormatter.error("OOM command not allowed when used memory > 'maxmemory'");
    }

    private byte[] handleDel(Command command) {
        if (command.getArgCount() < 1) {
            return RespFormatter.error("wrong number of arguments for 'del' command");
        }
        String[] keys = new String[command.getArgCount()];
        for (int i = 0; i < command.getArgCount(); i++) {
            keys[i] = command.getArgAsString(i);
        }
        long deletedCount = store.del(keys);
        return RespFormatter.integer(deletedCount);
    }

    private byte[] handleExpire(Command command) {
        if (command.getArgCount() < 2) {
            return RespFormatter.error("wrong number of arguments for 'expire' command");
        }
        String key = command.getArgAsString(0);
        long seconds = Long.parseLong(command.getArgAsString(1));
        boolean success = store.expire(key, seconds);
        return RespFormatter.integer(success ? 1 : 0);
    }

    private byte[] handlePExpire(Command command) {
        if (command.getArgCount() < 2) {
            return RespFormatter.error("wrong number of arguments for 'pexpire' command");
        }
        String key = command.getArgAsString(0);
        long millis = Long.parseLong(command.getArgAsString(1));
        boolean success = store.pexpire(key, millis);
        return RespFormatter.integer(success ? 1 : 0);
    }

    private byte[] handleExpireAt(Command command) {
        if (command.getArgCount() < 2) {
            return RespFormatter.error("wrong number of arguments for 'expireat' command");
        }
        String key = command.getArgAsString(0);
        long sec = Long.parseLong(command.getArgAsString(1));
        boolean success = store.expireAt(key, sec * 1000L);
        return RespFormatter.integer(success ? 1 : 0);
    }

    private byte[] handlePExpireAt(Command command) {
        if (command.getArgCount() < 2) {
            return RespFormatter.error("wrong number of arguments for 'pexpireat' command");
        }
        String key = command.getArgAsString(0);
        long ms = Long.parseLong(command.getArgAsString(1));
        boolean success = store.expireAt(key, ms);
        return RespFormatter.integer(success ? 1 : 0);
    }

    private byte[] handleTtl(Command command) {
        if (command.getArgCount() < 1) {
            return RespFormatter.error("wrong number of arguments for 'ttl' command");
        }
        String key = command.getArgAsString(0);
        return RespFormatter.integer(store.ttl(key));
    }

    private byte[] handlePttl(Command command) {
        if (command.getArgCount() < 1) {
            return RespFormatter.error("wrong number of arguments for 'pttl' command");
        }
        String key = command.getArgAsString(0);
        return RespFormatter.integer(store.pttl(key));
    }

    private byte[] handleExists(Command command) {
        if (command.getArgCount() < 1) {
            return RespFormatter.error("wrong number of arguments for 'exists' command");
        }
        long count = 0;
        for (int i = 0; i < command.getArgCount(); i++) {
            if (store.exists(command.getArgAsString(i))) {
                count++;
            }
        }
        return RespFormatter.integer(count);
    }

    private byte[] handleKeys(Command command) {
        String pattern = command.getArgCount() > 0 ? command.getArgAsString(0) : "*";
        List<String> keys = store.keys(pattern);
        return RespFormatter.stringArray(keys);
    }

    private byte[] handleDbSize(Command command) {
        return RespFormatter.integer(store.dbsize());
    }

    private byte[] handleFlushDb(Command command) {
        store.flushDb();
        return RespFormatter.OK_RESP;
    }

    private byte[] handlePing(Command command) {
        if (command.getArgCount() > 0) {
            return RespFormatter.bulkString(command.getArg(0));
        }
        return RespFormatter.PONG_RESP;
    }

    private byte[] handleCommandCompat(Command command) {
        return RespFormatter.EMPTY_ARRAY;
    }

    private byte[] handleInfo(Command command) {
        EngineStats stats = store.getStats();
        StringBuilder sb = new StringBuilder();
        sb.append("# Server\r\n");
        sb.append("kv_version:1.0.0\r\n");
        sb.append("uptime_in_seconds:").append(stats.getUptimeSeconds()).append("\r\n");
        sb.append("\r\n# Stats\r\n");
        sb.append("total_commands_processed:").append(stats.getTotalCommandsProcessed()).append("\r\n");
        sb.append("total_set_ops:").append(stats.getTotalSetOps()).append("\r\n");
        sb.append("total_del_ops:").append(stats.getTotalDelOps()).append("\r\n");
        sb.append("keyspace_hits:").append(stats.getGetHits()).append("\r\n");
        sb.append("keyspace_misses:").append(stats.getGetMisses()).append("\r\n");
        sb.append(String.format("hit_rate_pct:%.2f\r\n", stats.getHitRate()));
        sb.append("evicted_keys:").append(stats.getTotalEvictedKeys()).append("\r\n");
        sb.append("expired_keys:").append(stats.getTotalExpiredKeys()).append("\r\n");
        sb.append("\r\n# Memory & Keyspace\r\n");
        sb.append("used_memory:").append(store.memoryUsage()).append("\r\n");
        sb.append("total_keys:").append(store.dbsize()).append("\r\n");
        sb.append("shards:").append(store.getConfig().getShardCount()).append("\r\n");
        sb.append("eviction_policy:").append(store.getConfig().getEvictionPolicy()).append("\r\n");
        sb.append("aof_enabled:").append(store.getConfig().isAofEnabled()).append("\r\n");

        return RespFormatter.bulkString(sb.toString());
    }
}
