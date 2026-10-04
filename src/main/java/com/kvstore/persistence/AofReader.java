package com.kvstore.persistence;

import com.kvstore.engine.DataStore;
import com.kvstore.protocol.Command;
import com.kvstore.protocol.ProtocolParser;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Replays AOF / WAL log commands to restore in-memory state on startup.
 */
public final class AofReader {

    private AofReader() {}

    /**
     * Replays the given AOF file into the target DataStore.
     * Returns the total number of commands successfully replayed.
     */
    public static long replay(File aofFile, DataStore store) throws IOException {
        if (!aofFile.exists() || aofFile.length() == 0) {
            return 0;
        }

        long replayedCount = 0;
        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(aofFile))) {
            while (true) {
                Command cmd = ProtocolParser.parseCommand(bis);
                if (cmd == null) {
                    break; // EOF
                }

                executeReplayCommand(cmd, store);
                replayedCount++;
            }
        }
        return replayedCount;
    }

    private static void executeReplayCommand(Command cmd, DataStore store) {
        switch (cmd.getType()) {
            case SET -> {
                if (cmd.getArgCount() >= 2) {
                    String key = cmd.getArgAsString(0);
                    byte[] value = cmd.getArg(1);
                    long expireAtMillis = -1;

                    // Check for optional PXAT or EXAT or EX or PX
                    if (cmd.getArgCount() >= 4) {
                        String opt = cmd.getArgAsString(2);
                        String val = cmd.getArgAsString(3);
                        try {
                            if ("PXAT".equalsIgnoreCase(opt)) {
                                expireAtMillis = Long.parseLong(val);
                            } else if ("EXAT".equalsIgnoreCase(opt)) {
                                expireAtMillis = Long.parseLong(val) * 1000L;
                            } else if ("EX".equalsIgnoreCase(opt)) {
                                expireAtMillis = System.currentTimeMillis() + (Long.parseLong(val) * 1000L);
                            } else if ("PX".equalsIgnoreCase(opt)) {
                                expireAtMillis = System.currentTimeMillis() + Long.parseLong(val);
                            }
                        } catch (NumberFormatException ignored) {}
                    }
                    store.set(key, value, expireAtMillis);
                }
            }
            case DEL -> {
                for (int i = 0; i < cmd.getArgCount(); i++) {
                    store.del(cmd.getArgAsString(i));
                }
            }
            case EXPIRE -> {
                if (cmd.getArgCount() >= 2) {
                    String key = cmd.getArgAsString(0);
                    try {
                        long sec = Long.parseLong(cmd.getArgAsString(1));
                        store.expire(key, sec);
                    } catch (NumberFormatException ignored) {}
                }
            }
            case PEXPIRE -> {
                if (cmd.getArgCount() >= 2) {
                    String key = cmd.getArgAsString(0);
                    try {
                        long ms = Long.parseLong(cmd.getArgAsString(1));
                        store.pexpire(key, ms);
                    } catch (NumberFormatException ignored) {}
                }
            }
            case EXPIREAT -> {
                if (cmd.getArgCount() >= 2) {
                    String key = cmd.getArgAsString(0);
                    try {
                        long sec = Long.parseLong(cmd.getArgAsString(1));
                        store.expireAt(key, sec * 1000L);
                    } catch (NumberFormatException ignored) {}
                }
            }
            case PEXPIREAT -> {
                if (cmd.getArgCount() >= 2) {
                    String key = cmd.getArgAsString(0);
                    try {
                        long ms = Long.parseLong(cmd.getArgAsString(1));
                        store.expireAt(key, ms);
                    } catch (NumberFormatException ignored) {}
                }
            }
            case FLUSHDB -> store.flushDb();
            default -> {
                // Ignore read-only or unsupported commands during replay
            }
        }
    }
}
