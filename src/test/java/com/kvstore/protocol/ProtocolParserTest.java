package com.kvstore.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ProtocolParserTest {

    @Test
    void testInlineCommandParsing() throws Exception {
        String raw = "SET mykey myval\r\n";
        ByteArrayInputStream in = new ByteArrayInputStream(raw.getBytes(StandardCharsets.UTF_8));

        Command cmd = ProtocolParser.parseCommand(in);
        assertNotNull(cmd);
        assertEquals(CommandType.SET, cmd.getType());
        assertEquals(2, cmd.getArgCount());
        assertEquals("mykey", cmd.getArgAsString(0));
        assertEquals("myval", cmd.getArgAsString(1));
    }

    @Test
    void testRespArrayParsing() throws Exception {
        String raw = "*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$4\r\njohn\r\n";
        ByteArrayInputStream in = new ByteArrayInputStream(raw.getBytes(StandardCharsets.UTF_8));

        Command cmd = ProtocolParser.parseCommand(in);
        assertNotNull(cmd);
        assertEquals(CommandType.SET, cmd.getType());
        assertEquals(2, cmd.getArgCount());
        assertEquals("user", cmd.getArgAsString(0));
        assertEquals("john", cmd.getArgAsString(1));
    }

    @Test
    void testPipeliningMultipleCommands() throws Exception {
        String raw = "SET k1 v1\r\nSET k2 v2\r\nGET k1\r\n";
        ByteArrayInputStream in = new ByteArrayInputStream(raw.getBytes(StandardCharsets.UTF_8));

        Command c1 = ProtocolParser.parseCommand(in);
        assertNotNull(c1);
        assertEquals(CommandType.SET, c1.getType());
        assertEquals("k1", c1.getArgAsString(0));

        Command c2 = ProtocolParser.parseCommand(in);
        assertNotNull(c2);
        assertEquals(CommandType.SET, c2.getType());
        assertEquals("k2", c2.getArgAsString(0));

        Command c3 = ProtocolParser.parseCommand(in);
        assertNotNull(c3);
        assertEquals(CommandType.GET, c3.getType());
        assertEquals("k1", c3.getArgAsString(0));

        // EOF
        Command c4 = ProtocolParser.parseCommand(in);
        assertNull(c4);
    }
}
