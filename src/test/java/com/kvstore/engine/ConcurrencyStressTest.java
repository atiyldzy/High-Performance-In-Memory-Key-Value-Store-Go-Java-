package com.kvstore.engine;

import com.kvstore.config.ServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class ConcurrencyStressTest {
    private ShardedStorageEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        ServerConfig config = ServerConfig.createDefault();
        config.setShardCount(16);
        config.setAofEnabled(false);
        config.setActiveExpirationEnabled(false);
        engine = new ShardedStorageEngine(config);
        engine.init();
    }

    @AfterEach
    void tearDown() {
        if (engine != null) {
            engine.close();
        }
    }

    @Test
    void testHighConcurrencyReadWriteDelete() throws InterruptedException {
        int threadCount = 32;
        int operationsPerThread = 5_000;
        int keySpace = 500;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch finishGate = new CountDownLatch(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startGate.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        int keyIndex = ThreadLocalRandom.current().nextInt(keySpace);
                        String key = "stress:key:" + keyIndex;
                        int action = ThreadLocalRandom.current().nextInt(10);

                        if (action < 5) {
                            // 50% GET
                            byte[] val = engine.get(key);
                            if (val != null) {
                                String s = new String(val, StandardCharsets.UTF_8);
                                if (!s.startsWith("value-")) {
                                    errorCount.incrementAndGet();
                                }
                            }
                        } else if (action < 8) {
                            // 30% SET
                            engine.set(key, "value-" + keyIndex);
                        } else {
                            // 20% DEL
                            engine.del(key);
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                } finally {
                    finishGate.countDown();
                }
            });
        }

        startGate.countDown();
        finishGate.await();
        executor.shutdown();

        assertEquals(0, errorCount.get(), "No data race errors or thread safety violations should occur");
        assertTrue(engine.dbsize() >= 0);
        assertTrue(engine.getStats().getTotalCommandsProcessed() >= (long) threadCount * operationsPerThread);
    }
}
