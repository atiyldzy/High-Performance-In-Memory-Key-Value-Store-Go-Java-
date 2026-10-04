package com.kvstore.protocol;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * High-performance serializer for RESP (REdis Serialization Protocol) responses.
 */
public final class RespFormatter {
    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);
    public static final byte[] OK_RESP = "+OK\r\n".getBytes(StandardCharsets.UTF_8);
    public static final byte[] PONG_RESP = "+PONG\r\n".getBytes(StandardCharsets.UTF_8);
    public static final byte[] NULL_BULK_STRING = "$-1\r\n".getBytes(StandardCharsets.UTF_8);
    public static final byte[] EMPTY_ARRAY = "*0\r\n".getBytes(StandardCharsets.UTF_8);

    private RespFormatter() {}

    public static byte[] simpleString(String message) {
        return ("+" + message + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] error(String errorMessage) {
        return ("-ERR " + errorMessage + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] integer(long value) {
        return (":" + value + "\r\n").getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] bulkString(byte[] content) {
        if (content == null) {
            return NULL_BULK_STRING;
        }
        String prefix = "$" + content.length + "\r\n";
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        byte[] result = new byte[prefixBytes.length + content.length + CRLF.length];
        System.arraycopy(prefixBytes, 0, result, 0, prefixBytes.length);
        System.arraycopy(content, 0, result, prefixBytes.length, content.length);
        System.arraycopy(CRLF, 0, result, prefixBytes.length + content.length, CRLF.length);
        return result;
    }

    public static byte[] bulkString(String content) {
        if (content == null) {
            return NULL_BULK_STRING;
        }
        return bulkString(content.getBytes(StandardCharsets.UTF_8));
    }

    public static byte[] array(List<byte[]> bulkItems) {
        if (bulkItems == null) {
            return "*-1\r\n".getBytes(StandardCharsets.UTF_8);
        }
        if (bulkItems.isEmpty()) {
            return EMPTY_ARRAY;
        }

        StringBuilder header = new StringBuilder("*" + bulkItems.size() + "\r\n");
        int totalLength = header.toString().getBytes(StandardCharsets.UTF_8).length;

        byte[][] itemBytes = new byte[bulkItems.size()][];
        for (int i = 0; i < bulkItems.size(); i++) {
            itemBytes[i] = bulkString(bulkItems.get(i));
            totalLength += itemBytes[i].length;
        }

        byte[] out = new byte[totalLength];
        byte[] headerBytes = header.toString().getBytes(StandardCharsets.UTF_8);
        System.arraycopy(headerBytes, 0, out, 0, headerBytes.length);

        int offset = headerBytes.length;
        for (byte[] item : itemBytes) {
            System.arraycopy(item, 0, out, offset, item.length);
            offset += item.length;
        }
        return out;
    }

    public static byte[] stringArray(List<String> items) {
        if (items == null) {
            return "*-1\r\n".getBytes(StandardCharsets.UTF_8);
        }
        List<byte[]> byteItems = items.stream()
                .map(s -> s.getBytes(StandardCharsets.UTF_8))
                .toList();
        return array(byteItems);
    }
}
