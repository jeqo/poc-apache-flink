package poc;

import com.twitter.chill.java.UnmodifiableMapSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import java.nio.file.Files;
import java.nio.file.Path;

public class App {
    final StreamExecutionEnvironment env;

    public App(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void build() throws ClassNotFoundException {
        env.getConfig().registerKryoType(RemoteLogMetadata.class);
        env.getConfig().registerKryoType(RemoteLogSegmentMetadata.class);
        env.getConfig().registerKryoType(RemoteLogSegmentMetadataUpdate.class);

        // https://stackoverflow.com/a/32453031/4113777
        Class<?> unmodMapClass = Class.forName("java.util.Collections$UnmodifiableMap");
        env.getConfig().addDefaultKryoSerializer(unmodMapClass, UnmodifiableMapSerializer.class);

        final var deserializer = KafkaRecordDeserializationSchema.valueOnly(RemoteLogMetadataDeserializer.class);

        final var source = KafkaSource.<RemoteLogMetadata>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("__remote_log_metadata")
            .setGroupId("flink-app-v3")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(deserializer)
            .build();
        final var kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka:remote-log-metadata");

        final var started =
            kafkaSource
                .filter(remoteLogMetadata -> remoteLogMetadata instanceof RemoteLogSegmentMetadata)
                .map(remoteLogMetadata -> (RemoteLogSegmentMetadata) remoteLogMetadata)
                .filter(remoteLogSegmentMetadata -> remoteLogSegmentMetadata.state().equals(RemoteLogSegmentState.COPY_SEGMENT_STARTED))
                .keyBy(RemoteLogSegmentMetadata::remoteLogSegmentId);

        final var finished =
            kafkaSource
                .filter(remoteLogMetadata -> remoteLogMetadata instanceof RemoteLogSegmentMetadataUpdate)
                .map(remoteLogMetadata -> (RemoteLogSegmentMetadataUpdate) remoteLogMetadata)
                .filter(remoteLogSegmentMetadataUpdate -> remoteLogSegmentMetadataUpdate.state().equals(RemoteLogSegmentState.COPY_SEGMENT_FINISHED))
                .keyBy(RemoteLogSegmentMetadataUpdate::remoteLogSegmentId);

        final var merged =
            started.intervalJoin(finished)
                .between(Time.hours(-1), Time.hours(1))
                .process(new RemoteLogMetadataMerger())
                .process(new RemoteTierFetcher());

        merged.print();
    }

    static class RemoteTierFetcher extends ProcessFunction<RemoteLogSegmentMetadata, RemoteLogSegmentMetadata> {
        @Override
        public void processElement(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                   ProcessFunction<RemoteLogSegmentMetadata, RemoteLogSegmentMetadata>.Context ctx,
                                   Collector<RemoteLogSegmentMetadata> collector) throws Exception {
            final RemoteStorageManager rsm = RsmClient.build();

            try (final var out = Files.newOutputStream(Path.of("segment-" + remoteLogSegmentMetadata.remoteLogSegmentId().id().toString() + ".log"));
                 final var in = rsm.fetchLogSegment(remoteLogSegmentMetadata, 0)) {
                in.transferTo(out);
            } catch (RemoteResourceNotFoundException e) {
                //ignore
            }

            collector.collect(remoteLogSegmentMetadata);
        }
    }

    static class RemoteLogMetadataMerger
        extends ProcessJoinFunction<RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteLogSegmentMetadata> {
            @Override
            public void processElement(RemoteLogSegmentMetadata left,
                                       RemoteLogSegmentMetadataUpdate right,
                                       ProcessJoinFunction<RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteLogSegmentMetadata>.Context ctx,
                                       Collector<RemoteLogSegmentMetadata> out) {
                out.collect(left.createWithUpdates(right));
            }
    }

    // Run with 
    // --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED 
    public static void main(String[] args) throws Exception {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        final var app = new App(env);
        app.build();
        env.execute("test-kafka");
    }
}
