package com.kvstore.engine;

import com.kvstore.config.ServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class ActiveExpirationTest {
    private ShardedStorageEngine engine;

    @BeforeEach
    void setUp() throws Exception {
        ServerConfig config = ServerConfig.createDefault();
        config.setShardCount(4);
        config.setAofEnabled(false);
        config.setActiveExpirationEnabled(true);
        config.setActiveExpireIntervalMs(50); // fast background polling for test
        config.setActiveExpireSampleCount(50);
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
    void testActiveBackgroundExpiration() throws InterruptedException {
        // Populate 100 short-lived keys (TTL = 150ms)
        for (int i = 0; i < 100; i++) {
            engine.pexpire("expireKey:" + i, 150); // Note: we can set with setPx or set and pexpire
            engine.set("expireKey:" + i, ("val:" + i).getBytes(), System.currentTimeMillis() + 150);
        }

        assertEquals(100, engine.dbsize());

        // Sleep for 350ms to allow active background thread to sample and evict without any GET requests
        Thread.sleep(350);

        assertEquals(0, engine.dbsize(), "Active expiration daemon should have proactively purged expired keys");
        assertTrue(engine.getStats().getTotalExpiredKeys() >= 100);
    }
}
