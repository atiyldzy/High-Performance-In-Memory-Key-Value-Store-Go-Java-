package com.kvstore.protocol;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Encapsulation of a parsed client command with its arguments.
 */
public final class Command {
    private final CommandType type;
    private final List<byte[]> args;

    public Command(CommandType type, List<byte[]> args) {
        this.type = type;
        this.args = args != null ? args : Collections.emptyList();
    }

    public static Command of(CommandType type, List<byte[]> args) {
        return new Command(type, args);
    }

    public static Command of(CommandType type, String... strArgs) {
        List<byte[]> args = new ArrayList<>(strArgs.length);
        for (String s : strArgs) {
            args.add(s.getBytes(StandardCharsets.UTF_8));
        }
        return new Command(type, args);
    }

    public CommandType getType() {
        return type;
    }

    public List<byte[]> getArgs() {
        return args;
    }

    public int getArgCount() {
        return args.size();
    }

    public byte[] getArg(int index) {
        if (index < 0 || index >= args.size()) {
            return null;
        }
        return args.get(index);
    }

    public String getArgAsString(int index) {
        byte[] b = getArg(index);
        return b != null ? new String(b, StandardCharsets.UTF_8) : null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(type.name());
        for (byte[] arg : args) {
            sb.append(" ").append(new String(arg, StandardCharsets.UTF_8));
        }
        return sb.toString();
    }
}
