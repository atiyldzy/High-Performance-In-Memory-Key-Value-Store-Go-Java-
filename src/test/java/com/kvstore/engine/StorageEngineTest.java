package com.kvstore.engine;

import com.kvstore.config.ServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class StorageEngineTest {
    private ShardedStorageEngine engine;
    private ServerConfig config;

    @BeforeEach
    void setUp() throws Exception {
        config = ServerConfig.createDefault();
        config.setAofEnabled(false);
        config.setActiveExpirationEnabled(false);
        config.setShardCount(4);
        config.setMaxKeys(1000);
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
    void testBasicSetAndGet() {
        boolean setOk = engine.set("user:100", "Alice");
        assertTrue(setOk);

        byte[] valBytes = engine.get("user:100");
        assertNotNull(valBytes);
        assertEquals("Alice", new String(valBytes, StandardCharsets.UTF_8));

        // Non existent key
        assertNull(engine.get("user:non_existent"));
    }

    @Test
    void testDelete() {
        engine.set("k1", "v1");
        engine.set("k2", "v2");
        engine.set("k3", "v3");

        assertEquals(3, engine.dbsize());

        long deleted = engine.del("k1", "k3", "non_existent");
        assertEquals(2, deleted);

        assertNull(engine.get("k1"));
        assertNotNull(engine.get("k2"));
        assertNull(engine.get("k3"));
        assertEquals(1, engine.dbsize());
    }

    @Test
    void testExpireAndTtl() throws InterruptedException {
        engine.set("tempKey", "val");
        assertEquals(-1, engine.ttl("tempKey")); // Immortal

        boolean expSet = engine.expire("tempKey", 1); // 1 second
        assertTrue(expSet);

        long ttl = engine.ttl("tempKey");
        assertTrue(ttl >= 1, "TTL should be >= 1 second immediately after setting");

        // Sleep to let it expire
        Thread.sleep(1100);

        assertEquals(-2, engine.ttl("tempKey")); // Expired
        assertNull(engine.get("tempKey")); // Passive expiration triggers
        assertFalse(engine.exists("tempKey"));
    }

    @Test
    void testLruEviction() {
        // Configure engine with max 3 keys
        config.setMaxKeys(3);
        config.setEvictionPolicy(EvictionPolicy.LRU);
        config.setShardCount(1); // Single shard to test strict LRU chain
        ShardedStorageEngine lruEngine = new ShardedStorageEngine(config);

        lruEngine.set("k1", "v1");
        lruEngine.set("k2", "v2");
        lruEngine.set("k3", "v3");
        assertEquals(3, lruEngine.dbsize());

        // Access k1 and k2 to make them recently used, k3 becomes least recently used
        assertNotNull(lruEngine.get("k1"));
        assertNotNull(lruEngine.get("k2"));

        // Add k4 -> should evict k3
        lruEngine.set("k4", "v4");
        assertEquals(3, lruEngine.dbsize());

        assertNotNull(lruEngine.get("k1"));
        assertNotNull(lruEngine.get("k2"));
        assertNotNull(lruEngine.get("k4"));
        assertNull(lruEngine.get("k3")); // k3 was evicted

        lruEngine.close();
    }

    @Test
    void testKeysWildcardAndFlush() {
        engine.set("user:1", "u1");
        engine.set("user:2", "u2");
        engine.set("order:1", "o1");

        List<String> userKeys = engine.keys("user:*");
        assertEquals(2, userKeys.size());
        assertTrue(userKeys.contains("user:1"));
        assertTrue(userKeys.contains("user:2"));

        List<String> allKeys = engine.keys("*");
        assertEquals(3, allKeys.size());

        engine.flushDb();
        assertEquals(0, engine.dbsize());
        assertTrue(engine.keys("*").isEmpty());
    }
}
