package com.kvstore.benchmark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * High-resolution latency recorder and percentile statistics calculator.
 */
public final class LatencyStats {
    private final ConcurrentLinkedQueue<long[]> sampleBatches = new ConcurrentLinkedQueue<>();

    public void recordSamples(long[] latenciesNanos, int count) {
        long[] copy = Arrays.copyOf(latenciesNanos, count);
        sampleBatches.add(copy);
    }

    public Report generateReport(long totalElapsedNanos, long totalOps, long memoryAllocatedBytes) {
        // Flatten and sort all recorded sample latencies
        int totalRecorded = 0;
        for (long[] batch : sampleBatches) {
            totalRecorded += batch.length;
        }

        long[] all = new long[totalRecorded];
        int pos = 0;
        for (long[] batch : sampleBatches) {
            System.arraycopy(batch, 0, all, pos, batch.length);
            pos += batch.length;
        }

        Arrays.sort(all);

        double elapsedSeconds = totalElapsedNanos / 1_000_000_000.0;
        double throughputOpsPerSec = totalOps / Math.max(0.0001, elapsedSeconds);

        if (totalRecorded == 0) {
            return new Report(totalOps, elapsedSeconds, throughputOpsPerSec, 0, 0, 0, 0, 0, 0, 0, memoryAllocatedBytes);
        }

        long minNanos = all[0];
        long maxNanos = all[all.length - 1];
        long sum = 0;
        for (long val : all) {
            sum += val;
        }
        double avgNanos = (double) sum / all.length;

        double p50Nanos = percentile(all, 0.50);
        double p90Nanos = percentile(all, 0.90);
        double p95Nanos = percentile(all, 0.95);
        double p99Nanos = percentile(all, 0.99);
        double p999Nanos = percentile(all, 0.999);

        return new Report(
                totalOps,
                elapsedSeconds,
                throughputOpsPerSec,
                minNanos / 1_000_000.0,
                avgNanos / 1_000_000.0,
                maxNanos / 1_000_000.0,
                p50Nanos / 1_000_000.0,
                p90Nanos / 1_000_000.0,
                p95Nanos / 1_000_000.0,
                p99Nanos / 1_000_000.0,
                memoryAllocatedBytes
        );
    }

    private double percentile(long[] sorted, double pct) {
        int index = (int) Math.ceil(pct * sorted.length) - 1;
        index = Math.max(0, Math.min(sorted.length - 1, index));
        return sorted[index];
    }

    public record Report(
            long totalOperations,
            double durationSeconds,
            double throughputOpsSec,
            double minLatencyMs,
            double avgLatencyMs,
            double maxLatencyMs,
            double p50LatencyMs,
            double p90LatencyMs,
            double p95LatencyMs,
            double p99LatencyMs,
            long allocatedMemoryBytes
    ) {
        public void printSummary(String testName) {
            System.out.println("================================================================================");
            System.out.printf("  BENCHMARK REPORT: %s%n", testName);
            System.out.println("================================================================================");
            System.out.printf("  Total Operations    : %,d ops%n", totalOperations);
            System.out.printf("  Duration            : %.3f s%n", durationSeconds);
            System.out.printf("  Throughput          : %,.2f ops/sec%n", throughputOpsSec);
            System.out.printf("  Memory Delta        : %.2f MB%n", allocatedMemoryBytes / (1024.0 * 1024.0));
            System.out.println("--------------------------------------------------------------------------------");
            System.out.println("  LATENCY DISTRIBUTION (milliseconds):");
            System.out.printf("    Min Latency       : %8.3f ms%n", minLatencyMs);
            System.out.printf("    Avg Latency       : %8.3f ms%n", avgLatencyMs);
            System.out.printf("    p50 (Median)      : %8.3f ms%n", p50LatencyMs);
            System.out.printf("    p90               : %8.3f ms%n", p90LatencyMs);
            System.out.printf("    p95               : %8.3f ms%n", p95LatencyMs);
            System.out.printf("    p99               : %8.3f ms%n", p99LatencyMs);
            System.out.printf("    Max Latency       : %8.3f ms%n", maxLatencyMs);
            System.out.println("================================================================================\n");
        }
    }
}
