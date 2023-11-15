package poc;

import com.twitter.chill.java.UnmodifiableMapSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.server.log.remote.storage.RemoteLogMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadataUpdate;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentState;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class App {
    static final String TOPIC = "t2";
    final StreamExecutionEnvironment env;

    public App(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void build() throws ClassNotFoundException, RestClientException, IOException {
        env.getConfig().registerKryoType(RemoteLogMetadata.class);
        env.getConfig().registerKryoType(RemoteLogSegmentMetadata.class);
        env.getConfig().registerKryoType(RemoteLogSegmentMetadataUpdate.class);
        env.getConfig().enableForceAvro();

        // https://stackoverflow.com/a/32453031/4113777
        final var unmodMapClass = Class.forName("java.util.Collections$UnmodifiableMap");
        env.getConfig().addDefaultKryoSerializer(unmodMapClass, UnmodifiableMapSerializer.class);
        final var unmodCollectionClass = Class.forName("java.util.Collections$UnmodifiableCollection");
        env.getConfig().addDefaultKryoSerializer(unmodCollectionClass, UnmodifiableCollectionsSerializer.class);

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
                .process(new RemoteLogMetadataMerger());

        final var srClient = new CachedSchemaRegistryClient("http://localhost:8081", 100);
        final var schemas = srClient.getSchemas(TOPIC + "-value", false, true);
        final var schema = schemas.get(0);
        final var parse = Schema.parse(schema.canonicalString());

        final var processed =
            merged.process(new RemoteTierFetcher(), new GenericRecordAvroTypeInfo(parse));

        processed.print();
    }

    static class RemoteTierFetcher extends ProcessFunction<RemoteLogSegmentMetadata, GenericRecord> {
        @Override
        public void processElement(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                   ProcessFunction<RemoteLogSegmentMetadata, GenericRecord>.Context ctx,
                                   Collector<GenericRecord> collector) throws Exception {
            final RemoteStorageManager rsm = RsmClient.get();

            final var path = Path.of("segment-" + remoteLogSegmentMetadata.remoteLogSegmentId().id().toString() + ".log");
            try (final var out = Files.newOutputStream(path);
                 final var in = rsm.fetchLogSegment(remoteLogSegmentMetadata, 0)) {
                in.transferTo(out);
            } catch (RemoteResourceNotFoundException e) {
                //ignore
            }

            var converter = new AvroConverter();
            converter.configure(Map.of("schema.registry.url", "http://localhost:8081"), false);

            try (final var fileRecords = FileRecords.open(path.toFile());
                 final var serde = new SchemaAndValueSerde(converter)) {
                while (fileRecords.batchIterator().hasNext()) {
                    final var recordBatch = fileRecords.batchIterator().next();
                    try (final var records = recordBatch.streamingIterator(BufferSupplier.create())) {
                        while (records.hasNext()) {
                            final var record = records.next();
                            if (remoteLogSegmentMetadata.topicIdPartition().topic().equals(TOPIC)) {
                                byte[] dst = new byte[record.valueSize()];
                                record.value().get(dst);
                                final var schemaAndValue = serde.deserializer().deserialize(TOPIC, dst);
                                final var valueData = new AvroData(100).fromConnectData(schemaAndValue.schema(), schemaAndValue.value());

                                collector.collect((GenericRecord) valueData);
                            }
                        }
                    }
                }
            } finally {
                Files.deleteIfExists(path);
            }
        }
    }

    static class RemoteLogMetadataMerger
        extends ProcessJoinFunction<RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteLogSegmentMetadata> {
            @Override
            public void processElement(RemoteLogSegmentMetadata left,
                                       RemoteLogSegmentMetadataUpdate right,
                                       ProcessJoinFunction<RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemoteLogSegmentMetadata>.Context ctx,
                                       Collector<RemoteLogSegmentMetadata> out) {
                out. collect(left.createWithUpdates(right));
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
