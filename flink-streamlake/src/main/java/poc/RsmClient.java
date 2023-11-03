package poc;

import io.aiven.kafka.tieredstorage.chunkmanager.cache.InMemoryChunkCache;
import io.aiven.kafka.tieredstorage.storage.s3.S3Storage;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import java.util.HashMap;

public class RsmClient {

    public static RemoteStorageManager build() {
        final var rsm = new io.aiven.kafka.tieredstorage.RemoteStorageManager();
        final var configs = new HashMap<String, Object>();
        configs.put("chunk.size", 5242880);
        configs.put("chunk.cache.class", InMemoryChunkCache.class);
        configs.put("chunk.cache.size", -1);
        configs.put("custom.metadata.fields.include", "REMOTE_SIZE");
        configs.put("key.prefix", "tiered-storage-demo/");
        configs.put("storage.backend.class", S3Storage.class);
        configs.put("storage.s3.endpoint.url", "http://localhost:9000");
        configs.put("storage.s3.bucket.name", "test-bucket");
        configs.put("storage.s3.region", "us-east-1");
        configs.put("storage.s3.path.style.access.enabled", true);
        configs.put("storage.aws.access.key.id", "minioadmin");
        configs.put("storage.aws.secret.access.key", "minioadmin");
        rsm.configure(configs);
        return rsm;
    }
}
