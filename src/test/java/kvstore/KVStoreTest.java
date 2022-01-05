package kvstore;

import org.junit.Assert;
import org.junit.Test;
import wal.WALConfig;

public class KVStoreTest {

    @Test
    public void shouldWriteAndReadEntries() {
        KVStore kvStore = new KVStore(new WALConfig());

        kvStore.put("key1", "test content");
        kvStore.put("key2", "test content2");

        KVStore kvStore1 = new KVStore(new WALConfig());
        Assert.assertEquals("test content", kvStore1.get("key1"));
        Assert.assertEquals("test content2", kvStore1.get("key2"));
    }
}
