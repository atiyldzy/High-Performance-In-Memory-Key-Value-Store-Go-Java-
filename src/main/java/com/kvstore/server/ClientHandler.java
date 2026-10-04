package com.kvstore.server;

import com.kvstore.protocol.Command;
import com.kvstore.protocol.CommandType;
import com.kvstore.protocol.ProtocolParser;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

/**
 * Handles individual client TCP connections using Java 21 Virtual Threads.
 * Supports command pipelining and low-latency response batching.
 */
public final class ClientHandler implements Runnable {
    private static final int BUFFER_SIZE = 64 * 1024; // 64KB stream buffers
    private final Socket socket;
    private final CommandDispatcher dispatcher;

    public ClientHandler(Socket socket, CommandDispatcher dispatcher) {
        this.socket = socket;
        this.dispatcher = dispatcher;
    }

    @Override
    public void run() {
        try {
            socket.setTcpNoDelay(true);
            socket.setReceiveBufferSize(BUFFER_SIZE);
            socket.setSendBufferSize(BUFFER_SIZE);

            try (BufferedInputStream in = new BufferedInputStream(socket.getInputStream(), BUFFER_SIZE);
                 BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream(), BUFFER_SIZE)) {

                while (!socket.isClosed()) {
                    Command cmd;
                    try {
                        cmd = ProtocolParser.parseCommand(in);
                    } catch (EOFException e) {
                        break; // Normal client disconnect
                    } catch (SocketException e) {
                        break; // Connection reset/closed
                    } catch (IOException e) {
                        // Protocol error, write error and break
                        out.write(("-ERR " + e.getMessage() + "\r\n").getBytes());
                        out.flush();
                        break;
                    }

                    if (cmd == null) {
                        break; // Clean EOF
                    }

                    if (cmd.getType() == CommandType.QUIT) {
                        out.write("+OK\r\n".getBytes());
                        out.flush();
                        break;
                    }

                    // Execute command against dispatcher
                    byte[] response = dispatcher.execute(cmd);
                    out.write(response);

                    // Flush output when no more immediate bytes are waiting in the socket buffer (Pipelining optimization)
                    if (in.available() == 0) {
                        out.flush();
                    }
                }
                out.flush();
            }
        } catch (IOException ignored) {
            // Client disconnected
        } finally {
            try {
                if (!socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException ignored) {}
        }
    }
}
