package com.kvstore.server;

import com.kvstore.config.ServerConfig;
import com.kvstore.engine.ShardedStorageEngine;
import com.kvstore.persistence.AofReader;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Custom TCP Server engine powered by Java 21 Virtual Threads.
 * Capable of managing thousands of concurrent clients with minimal thread overhead.
 */
public final class TcpServer implements AutoCloseable {
    private final ServerConfig config;
    private final ShardedStorageEngine engine;
    private final CommandDispatcher dispatcher;
    private final ExecutorService clientExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    private ServerSocket serverSocket;
    private Thread acceptThread;

    public TcpServer(ServerConfig config) {
        this.config = config;
        this.engine = new ShardedStorageEngine(config);
        this.dispatcher = new CommandDispatcher(engine);
        this.clientExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public synchronized void start() throws IOException {
        if (running.compareAndSet(false, true)) {
            // Step 1: Initialize Engine
            engine.init();

            // Step 2: AOF Recovery Replay (if file exists)
            if (config.isAofEnabled()) {
                File aofFile = new File(config.getAofFilePath());
                if (aofFile.exists() && aofFile.length() > 0) {
                    System.out.println("[SERVER] Loading AOF file: " + aofFile.getAbsolutePath());
                    long replayed = AofReader.replay(aofFile, engine);
                    System.out.println("[SERVER] Successfully replayed " + replayed + " mutation commands from AOF.");
                }
            }

            // Step 3: Bind Server Socket
            this.serverSocket = new ServerSocket();
            this.serverSocket.setReuseAddress(true);
            this.serverSocket.bind(new InetSocketAddress(config.getHost(), config.getPort()), 1024);

            System.out.printf("[SERVER] KVStore Server started on %s:%d (Shards: %d, MaxMemory: %d MB, VirtualThreads: Enabled)%n",
                    config.getHost(), config.getPort(), config.getShardCount(), config.getMaxMemoryBytes() / (1024 * 1024));

            // Step 4: Accept Loop
            this.acceptThread = new Thread(this::acceptConnections, "kv-accept-worker");
            this.acceptThread.start();
        }
    }

    private void acceptConnections() {
        while (running.get()) {
            try {
                Socket clientSocket = serverSocket.accept();
                clientExecutor.submit(new ClientHandler(clientSocket, dispatcher));
            } catch (IOException e) {
                if (!running.get()) {
                    break; // Normal server shutdown
                }
                System.err.println("[SERVER] Error accepting client connection: " + e.getMessage());
            }
        }
    }

    public ShardedStorageEngine getEngine() {
        return engine;
    }

    public CommandDispatcher getDispatcher() {
        return dispatcher;
    }

    public ServerConfig getConfig() {
        return config;
    }

    public boolean isRunning() {
        return running.get();
    }

    @Override
    public synchronized void close() {
        if (running.compareAndSet(true, false)) {
            System.out.println("[SERVER] Shutting down KVStore Server...");
            try {
                if (serverSocket != null && !serverSocket.isClosed()) {
                    serverSocket.close();
                }
            } catch (IOException ignored) {}

            clientExecutor.shutdownNow();
            engine.close();
            System.out.println("[SERVER] KVStore Server shutdown complete.");
        }
    }
}
