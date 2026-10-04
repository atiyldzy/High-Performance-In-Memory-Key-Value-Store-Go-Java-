package com.kvstore.persistence;

import com.kvstore.config.ServerConfig;
import com.kvstore.engine.ShardedStorageEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class AofPersistenceTest {

    @Test
    void testAofWriteAndReplay(@TempDir Path tempDir) throws Exception {
        File aofFile = tempDir.resolve("test_appendonly.aof").toFile();

        ServerConfig config = ServerConfig.createDefault();
        config.setAofEnabled(true);
        config.setAofFilePath(aofFile.getAbsolutePath());
        config.setAofSyncPolicy(AofSyncPolicy.ALWAYS);
        config.setShardCount(4);

        // Step 1: Initialize engine, write data, close
        try (ShardedStorageEngine engine = new ShardedStorageEngine(config)) {
            engine.init();
            engine.set("site", "antigravity");
            engine.set("cluster", "prod-eu-1");
            engine.set("temp", "volatile-data");
            engine.del("temp");
            engine.expire("site", 60); // 60 seconds TTL

            // Allow async flush to complete
            Thread.sleep(200);
        }

        assertTrue(aofFile.exists());
        assertTrue(aofFile.length() > 0, "AOF file should not be empty");

        // Step 2: Start new fresh engine and replay
        ServerConfig replayConfig = ServerConfig.createDefault();
        replayConfig.setAofEnabled(false); // disable writing during replay test
        replayConfig.setShardCount(4);

        try (ShardedStorageEngine restoredEngine = new ShardedStorageEngine(replayConfig)) {
            restoredEngine.init();

            long count = AofReader.replay(aofFile, restoredEngine);
            assertTrue(count >= 3, "Should have replayed logged mutations");

            byte[] siteVal = restoredEngine.get("site");
            assertNotNull(siteVal);
            assertEquals("antigravity", new String(siteVal, StandardCharsets.UTF_8));

            byte[] clusterVal = restoredEngine.get("cluster");
            assertNotNull(clusterVal);
            assertEquals("prod-eu-1", new String(clusterVal, StandardCharsets.UTF_8));

            // Deleted key should be absent
            assertNull(restoredEngine.get("temp"));

            // TTL check
            long ttl = restoredEngine.ttl("site");
            assertTrue(ttl > 0 && ttl <= 60, "TTL should be restored");
        }
    }
}
