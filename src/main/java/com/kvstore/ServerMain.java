package com.kvstore;

import com.kvstore.benchmark.BenchmarkRunner;
import com.kvstore.config.ServerConfig;
import com.kvstore.engine.EvictionPolicy;
import com.kvstore.persistence.AofSyncPolicy;
import com.kvstore.server.TcpServer;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

/**
 * Main application entry point for KVStore:
 * Supports Server Daemon Mode, Interactive CLI Client, and Benchmark Runner.
 */
public final class ServerMain {

    public static void main(String[] args) {
        if (args.length > 0) {
            String mode = args[0].toLowerCase();
            switch (mode) {
                case "cli" -> {
                    runCli(args);
                    return;
                }
                case "benchmark" -> {
                    runBenchmark(args);
                    return;
                }
                case "help", "--help", "-h" -> {
                    printUsage();
                    return;
                }
            }
        }

        // Default: Start Server
        runServer(args);
    }

    private static void runServer(String[] args) {
        ServerConfig config = ServerConfig.createDefault();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (("--port".equals(arg) || "-p".equals(arg)) && i + 1 < args.length) {
                config.setPort(Integer.parseInt(args[++i]));
            } else if (("--host".equals(arg) || "-h".equals(arg)) && i + 1 < args.length) {
                config.setHost(args[++i]);
            } else if (("--shards".equals(arg) || "-s".equals(arg)) && i + 1 < args.length) {
                config.setShardCount(Integer.parseInt(args[++i]));
            } else if ("--max-memory-mb".equals(arg) && i + 1 < args.length) {
                config.setMaxMemoryBytes(Long.parseLong(args[++i]) * 1024 * 1024L);
            } else if ("--max-keys".equals(arg) && i + 1 < args.length) {
                config.setMaxKeys(Long.parseLong(args[++i]));
            } else if ("--aof".equals(arg) && i + 1 < args.length) {
                config.setAofEnabled(Boolean.parseBoolean(args[++i]));
            } else if ("--aof-path".equals(arg) && i + 1 < args.length) {
                config.setAofFilePath(args[++i]);
            } else if ("--aof-sync".equals(arg) && i + 1 < args.length) {
                config.setAofSyncPolicy(AofSyncPolicy.valueOf(args[++i].toUpperCase()));
            } else if ("--eviction".equals(arg) && i + 1 < args.length) {
                config.setEvictionPolicy(EvictionPolicy.valueOf(args[++i].toUpperCase()));
            }
        }

        try {
            TcpServer server = new TcpServer(config);
            server.start();

            // Register shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(server::close));

            System.out.println("================================================================================");
            System.out.println("   _  ____     ______  __                 ___     ____  ");
            System.out.println("  | |/ /\\ \\   / / ___| \\ \\      _____    / _ \\   / ___| ");
            System.out.println("  | ' /  \\ \\ / /\\___ \\  \\ \\ /\\ / / _ \\  | | | | | |  _  ");
            System.out.println("  | . \\   \\ V /  ___) |  \\ V  V / (_) | | |_| |_| |_| | ");
            System.out.println("  |_|\\_\\   \\_/  |____/    \\_/\\_/ \\___/   \\___/(_)____/  ");
            System.out.println("  High-Performance In-Memory Sharded Key-Value Database Engine");
            System.out.println("================================================================================");
            System.out.printf("  * Server running on %s:%d%n", config.getHost(), config.getPort());
            System.out.printf("  * Storage Shards   : %d%n", config.getShardCount());
            System.out.printf("  * Max Memory       : %d MB%n", config.getMaxMemoryBytes() / (1024 * 1024));
            System.out.printf("  * Eviction Policy  : %s%n", config.getEvictionPolicy());
            System.out.printf("  * AOF Persistence  : %s (Sync: %s)%n", config.isAofEnabled() ? "ENABLED (" + config.getAofFilePath() + ")" : "DISABLED", config.getAofSyncPolicy());
            System.out.println("  * Ready to accept connections. Press Ctrl+C to terminate.");
            System.out.println("================================================================================\n");

            // Keep main thread alive
            Thread.currentThread().join();
        } catch (Exception e) {
            System.err.println("[FATAL] Failed to start server: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void runCli(String[] args) {
        String host = "127.0.0.1";
        int port = 6379;
        for (int i = 1; i < args.length; i++) {
            if (("-h".equals(args[i]) || "--host".equals(args[i])) && i + 1 < args.length) {
                host = args[++i];
            } else if (("-p".equals(args[i]) || "--port".equals(args[i])) && i + 1 < args.length) {
                port = Integer.parseInt(args[++i]);
            }
        }

        System.out.printf("Connecting to KVStore CLI at %s:%d...%n", host, port);
        try (Socket socket = new Socket(host, port);
             BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream());
             InputStream in = socket.getInputStream();
             Scanner scanner = new Scanner(System.in)) {

            socket.setTcpNoDelay(true);
            System.out.println("Connected. Type commands (e.g. SET foo bar, GET foo, PING, INFO, QUIT).");

            while (true) {
                System.out.printf("%s:%d> ", host, port);
                if (!scanner.hasNextLine()) break;
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) continue;

                if ("exit".equalsIgnoreCase(line) || "quit".equalsIgnoreCase(line)) {
                    out.write("QUIT\r\n".getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    System.out.println("Bye!");
                    break;
                }

                out.write((line + "\r\n").getBytes(StandardCharsets.UTF_8));
                out.flush();

                // Read and format response
                printCliResponse(in);
            }
        } catch (Exception e) {
            System.err.println("[CLI ERROR] " + e.getMessage());
        }
    }

    private static void printCliResponse(InputStream in) throws Exception {
        int first = in.read();
        if (first == -1) {
            System.out.println("(connection closed)");
            return;
        }

        switch (first) {
            case '+' -> System.out.println(readLine(in)); // Simple string
            case '-' -> System.out.println("(error) " + readLine(in)); // Error
            case ':' -> System.out.println("(integer) " + readLine(in)); // Integer
            case '$' -> { // Bulk string
                String lenStr = readLine(in);
                int len = Integer.parseInt(lenStr.trim());
                if (len < 0) {
                    System.out.println("(nil)");
                } else {
                    byte[] data = in.readNBytes(len);
                    in.readNBytes(2); // Consume \r\n
                    System.out.println("\"" + new String(data, StandardCharsets.UTF_8) + "\"");
                }
            }
            case '*' -> { // Array
                String countStr = readLine(in);
                int count = Integer.parseInt(countStr.trim());
                if (count < 0) {
                    System.out.println("(nil)");
                } else if (count == 0) {
                    System.out.println("(empty array)");
                } else {
                    for (int i = 0; i < count; i++) {
                        System.out.printf("%d) ", (i + 1));
                        printCliResponse(in);
                    }
                }
            }
            default -> System.out.println((char) first + readLine(in));
        }
    }

    private static String readLine(InputStream in) throws Exception {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') break;
            if (b != '\r') sb.append((char) b);
        }
        return sb.toString();
    }

    private static void runBenchmark(String[] args) {
        String host = "127.0.0.1";
        int port = 6379;
        int clients = 50;
        int requests = 100_000;
        int pipeline = 1;
        int dataSize = 32;
        String workload = "mixed";

        for (int i = 1; i < args.length; i++) {
            String a = args[i];
            if ("-h".equals(a) && i + 1 < args.length) host = args[++i];
            else if ("-p".equals(a) && i + 1 < args.length) port = Integer.parseInt(args[++i]);
            else if ("-c".equals(a) && i + 1 < args.length) clients = Integer.parseInt(args[++i]);
            else if ("-n".equals(a) && i + 1 < args.length) requests = Integer.parseInt(args[++i]);
            else if ("-P".equals(a) && i + 1 < args.length) pipeline = Integer.parseInt(args[++i]);
            else if ("-d".equals(a) && i + 1 < args.length) dataSize = Integer.parseInt(args[++i]);
            else if ("-t".equals(a) && i + 1 < args.length) workload = args[++i];
        }

        try {
            BenchmarkRunner runner = new BenchmarkRunner(host, port, clients, requests, pipeline, dataSize, workload);
            runner.run();
        } catch (Exception e) {
            System.err.println("[BENCHMARK ERROR] " + e.getMessage());
        }
    }

    private static void printUsage() {
        System.out.println("KVStore Engine - High-Performance In-Memory Key-Value Database");
        System.out.println("\nUsage Modes:");
        System.out.println("  java -jar kv-engine.jar [server options]             # Starts server");
        System.out.println("  java -jar kv-engine.jar cli [-h host] [-p port]      # Interactive CLI");
        System.out.println("  java -jar kv-engine.jar benchmark [benchmark options]# Benchmarking tool");
        System.out.println("\nServer Options:");
        System.out.println("  --host <ip>             Bind host (default: 0.0.0.0)");
        System.out.println("  --port <port>           Listen port (default: 6379)");
        System.out.println("  --shards <n>            Number of concurrency lock shards (default: 64)");
        System.out.println("  --max-memory-mb <mb>    Maximum memory in MB before eviction (default: 512)");
        System.out.println("  --max-keys <n>          Maximum keys count before eviction (default: 1000000)");
        System.out.println("  --eviction <LRU|NOEVICTION> Eviction policy (default: LRU)");
        System.out.println("  --aof <true|false>      Enable Write-Ahead AOF Persistence (default: true)");
        System.out.println("  --aof-path <path>       AOF file destination (default: appendonly.aof)");
        System.out.println("  --aof-sync <ALWAYS|EVERYSEC|NO> AOF fsync policy (default: EVERYSEC)");
        System.out.println("\nBenchmark Options:");
        System.out.println("  -h <host>               Target host (default: 127.0.0.1)");
        System.out.println("  -p <port>               Target port (default: 6379)");
        System.out.println("  -c <clients>            Number of parallel client workers (default: 50)");
        System.out.println("  -n <requests>           Total operations count (default: 100000)");
        System.out.println("  -P <pipeline>           Pipeline depth (default: 1)");
        System.out.println("  -d <size>               Value payload size in bytes (default: 32)");
        System.out.println("  -t <set|get|mixed>      Workload type (default: mixed)");
    }
}
