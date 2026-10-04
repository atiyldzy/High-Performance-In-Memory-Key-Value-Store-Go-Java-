package com.kvstore.benchmark;

import com.kvstore.protocol.Command;
import com.kvstore.protocol.ProtocolParser;
import com.kvstore.protocol.RespFormatter;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

/**
 * High-concurrency client benchmark tool (comparable to redis-benchmark).
 */
public final class BenchmarkRunner {
    private final String host;
    private final int port;
    private final int clients;
    private final int totalRequests;
    private final int pipelineDepth;
    private final int valueSize;
    private final String workload; // "set", "get", "mixed"

    public BenchmarkRunner(String host, int port, int clients, int totalRequests, int pipelineDepth, int valueSize, String workload) {
        this.host = host;
        this.port = port;
        this.clients = Math.max(1, clients);
        this.totalRequests = totalRequests;
        this.pipelineDepth = Math.max(1, pipelineDepth);
        this.valueSize = Math.max(1, valueSize);
        this.workload = workload != null ? workload.toLowerCase() : "mixed";
    }

    public LatencyStats.Report run() throws InterruptedException {
        System.out.printf("Starting benchmark on %s:%d [%s] -> %d clients, %d total ops, pipeline=%d, data_size=%d bytes%n",
                host, port, workload.toUpperCase(), clients, totalRequests, pipelineDepth, valueSize);

        // Pre-populate dummy value payload
        byte[] payloadBytes = new byte[valueSize];
        Arrays.fill(payloadBytes, (byte) 'x');
        String payloadStr = new String(payloadBytes, StandardCharsets.UTF_8);

        // Garbage collection hint to measure allocation delta cleanly
        System.gc();
        Thread.sleep(100);
        long memBefore = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        LatencyStats latencyStats = new LatencyStats();
        int opsPerClient = totalRequests / clients;
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finishGate = new CountDownLatch(clients);

        ExecutorService executor = Executors.newFixedThreadPool(clients);

        for (int i = 0; i < clients; i++) {
            final int clientId = i;
            executor.submit(() -> {
                try (Socket socket = new Socket(host, port)) {
                    socket.setTcpNoDelay(true);
                    BufferedOutputStream out = new BufferedOutputStream(socket.getOutputStream(), 64 * 1024);
                    BufferedInputStream in = new BufferedInputStream(socket.getInputStream(), 64 * 1024);

                    startGate.await();

                    long[] latencies = new long[opsPerClient];
                    int sampleIdx = 0;

                    int opsRemaining = opsPerClient;
                    while (opsRemaining > 0) {
                        int batchSize = Math.min(opsRemaining, pipelineDepth);
                        long batchStartNanos = System.nanoTime();

                        // Write batch
                        for (int b = 0; b < batchSize; b++) {
                            int keyNum = ThreadLocalRandom.current().nextInt(totalRequests / 2);
                            String key = "bench:key:" + keyNum;

                            boolean isSet;
                            if ("set".equals(workload)) {
                                isSet = true;
                            } else if ("get".equals(workload)) {
                                isSet = false;
                            } else {
                                isSet = ThreadLocalRandom.current().nextInt(100) < 20; // 20% SET, 80% GET
                            }

                            if (isSet) {
                                String cmd = String.format("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
                                        key.length(), key, payloadStr.length(), payloadStr);
                                out.write(cmd.getBytes(StandardCharsets.UTF_8));
                            } else {
                                String cmd = String.format("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", key.length(), key);
                                out.write(cmd.getBytes(StandardCharsets.UTF_8));
                            }
                        }
                        out.flush();

                        // Read batch responses
                        for (int b = 0; b < batchSize; b++) {
                            readResponse(in);
                        }

                        long batchEndNanos = System.nanoTime();
                        long avgOpNanos = (batchEndNanos - batchStartNanos) / batchSize;
                        for (int b = 0; b < batchSize; b++) {
                            latencies[sampleIdx++] = avgOpNanos;
                        }

                        opsRemaining -= batchSize;
                    }

                    latencyStats.recordSamples(latencies, sampleIdx);
                } catch (Exception e) {
                    System.err.printf("[CLIENT-%d-ERROR] %s%n", clientId, e.getMessage());
                } finally {
                    finishGate.countDown();
                }
            });
        }

        long startNanos = System.nanoTime();
        startGate.countDown();
        finishGate.await();
        long totalElapsedNanos = System.nanoTime() - startNanos;

        executor.shutdown();

        long memAfter = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long memDelta = Math.max(0, memAfter - memBefore);

        LatencyStats.Report report = latencyStats.generateReport(totalElapsedNanos, (long) opsPerClient * clients, memDelta);
        report.printSummary("BENCHMARK - " + workload.toUpperCase());
        return report;
    }

    private static void readResponse(BufferedInputStream in) throws IOException {
        int first = in.read();
        if (first == -1) {
            throw new IOException("Connection closed prematurely");
        }
        if (first == '+' || first == '-' || first == ':') {
            // Read line
            readLine(in);
        } else if (first == '$') {
            String lenStr = readLine(in);
            int len = Integer.parseInt(lenStr.trim());
            if (len >= 0) {
                in.readNBytes(len);
                in.readNBytes(2); // \r\n
            }
        } else if (first == '*') {
            String countStr = readLine(in);
            int count = Integer.parseInt(countStr.trim());
            for (int i = 0; i < count; i++) {
                readResponse(in);
            }
        }
    }

    private static String readLine(BufferedInputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') break;
            if (b != '\r') sb.append((char) b);
        }
        return sb.toString();
    }
}
