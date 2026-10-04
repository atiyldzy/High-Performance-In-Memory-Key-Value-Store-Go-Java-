package com.kvstore.protocol;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Streaming protocol parser supporting both Redis RESP protocol and inline text commands.
 * Fully supports command pipelining over TCP connections.
 */
public final class ProtocolParser {

    private ProtocolParser() {}

    /**
     * Reads the next complete Command from the InputStream.
     * Returns null on clean EOF.
     * Throws IOException on protocol violation or network error.
     */
    public static Command parseCommand(InputStream in) throws IOException {
        int firstByte = in.read();
        if (firstByte == -1) {
            return null; // Clean EOF
        }

        if (firstByte == '*') {
            // RESP Array format: *<num_elements>\r\n...
            return parseRespArray(in);
        } else {
            // Inline text command format: e.g. "SET foo bar\r\n" or "PING\r\n"
            return parseInlineCommand(in, (byte) firstByte);
        }
    }

    /**
     * Parses a RESP array of bulk strings.
     */
    private static Command parseRespArray(InputStream in) throws IOException {
        String numArgsStr = readLine(in);
        int numArgs;
        try {
            numArgs = Integer.parseInt(numArgsStr.trim());
        } catch (NumberFormatException e) {
            throw new IOException("Protocol Error: invalid array length: " + numArgsStr);
        }

        if (numArgs <= 0) {
            return new Command(CommandType.UNKNOWN, List.of());
        }

        List<byte[]> args = new ArrayList<>(numArgs);
        for (int i = 0; i < numArgs; i++) {
            int marker = in.read();
            if (marker == -1) {
                throw new EOFException("Unexpected EOF while reading RESP element");
            }
            if (marker != '$') {
                throw new IOException("Protocol Error: expected '$', got '" + (char) marker + "'");
            }

            String lenStr = readLine(in);
            int len;
            try {
                len = Integer.parseInt(lenStr.trim());
            } catch (NumberFormatException e) {
                throw new IOException("Protocol Error: invalid bulk string length: " + lenStr);
            }

            if (len < 0) {
                args.add(null);
            } else {
                byte[] data = in.readNBytes(len);
                if (data.length < len) {
                    throw new EOFException("Unexpected EOF while reading bulk string body");
                }
                // Read trailing CRLF
                int cr = in.read();
                int lf = in.read();
                if (cr != '\r' || lf != '\n') {
                    // Be tolerant of single \n if not strictly \r\n
                    if (cr == '\n') {
                        // single newline handled
                    } else {
                        throw new IOException("Protocol Error: expected CRLF after bulk string");
                    }
                }
                args.add(data);
            }
        }

        if (args.isEmpty()) {
            return new Command(CommandType.UNKNOWN, List.of());
        }

        String commandName = new String(args.get(0), StandardCharsets.UTF_8);
        CommandType type = CommandType.fromString(commandName);
        List<byte[]> subArgs = args.subList(1, args.size());
        return new Command(type, subArgs);
    }

    /**
     * Parses an inline text command (e.g. "SET foo bar" or "GET foo").
     */
    private static Command parseInlineCommand(InputStream in, byte firstByte) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(firstByte);

        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                break;
            }
            if (b != '\r') {
                baos.write(b);
            }
        }

        String line = baos.toString(StandardCharsets.UTF_8).trim();
        if (line.isEmpty()) {
            return new Command(CommandType.UNKNOWN, List.of());
        }

        // Split by whitespace respecting quotes
        List<String> tokens = splitTokens(line);
        if (tokens.isEmpty()) {
            return new Command(CommandType.UNKNOWN, List.of());
        }

        CommandType type = CommandType.fromString(tokens.get(0));
        List<byte[]> args = new ArrayList<>(tokens.size() - 1);
        for (int i = 1; i < tokens.size(); i++) {
            args.add(tokens.get(i).getBytes(StandardCharsets.UTF_8));
        }

        return new Command(type, args);
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') {
                break;
            }
            if (b != '\r') {
                baos.write(b);
            }
        }
        return baos.toString(StandardCharsets.UTF_8);
    }

    private static List<String> splitTokens(String text) {
        List<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = 0;

        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (inQuotes) {
                if (c == quoteChar) {
                    inQuotes = false;
                } else {
                    current.append(c);
                }
            } else {
                if (c == '\'' || c == '"') {
                    inQuotes = true;
                    quoteChar = c;
                } else if (Character.isWhitespace(c)) {
                    if (current.length() > 0) {
                        tokens.add(current.toString());
                        current.setLength(0);
                    }
                } else {
                    current.append(c);
                }
            }
        }
        if (current.length() > 0) {
            tokens.add(current.toString());
        }
        return tokens;
    }
}
