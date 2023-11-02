package poc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class App {
    final StreamExecutionEnvironment env;

    public App(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public void build() {
        final var source = KafkaSource.<String>builder()
            .setBootstrapServers("localhost:9092")
            .setTopics("topic1")
            .setGroupId("my-group")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
        final var kafkaSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        final var serializer = KafkaRecordSerializationSchema.builder()
            .setValueSerializationSchema(new SimpleStringSchema())
            .setTopic("output-topic1")
            .build();

        final var kafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers("localhost:9092")
            .setRecordSerializer(serializer)
            .build();

        kafkaSource.sinkTo(kafkaSink);
    }

    public static void main(String[] args) throws Exception {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();
        final var app = new App(env);
        app.build();
        env.execute("test-kafka");

//		// Split up the lines in pairs (2-tuples) containing: (word,1)
//        DataStream<String> counts = text.flatMap(new Tokenizer())
//		// Group by the tuple field "0" and sum up tuple field "1"
//		.keyBy(value -> value.f0)
//		.sum(1)
//		.flatMap(new Reducer());
//
//		// Add the sink to so results
//		// are written to the outputTopic
//        counts.sinkTo(sink);

        // Execute program
//
//		var dataStream = env
//            .socketTextStream("localhost", 9999)
//            .flatMap(new Splitter())
//            .keyBy(value -> value.f0)
//            .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//            .sum(1);
//
//        dataStream.print();
//
//        env.execute("Window WordCount");
    }


    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into multiple pairs in the
     * form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer
        implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // Emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }


    // Implements a simple reducer using FlatMap to
    // reduce the Tuple2 into a single string for 
    // writing to kafka topics
    public static final class Reducer
        implements FlatMapFunction<Tuple2<String, Integer>, String> {

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<String> out) {
            // Convert the pairs to a string
            // for easy writing to Kafka Topic
            String count = value.f0 + " " + value.f1;
            out.collect(count);
        }
    }

    static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) {
            for (String word : sentence.split(" ")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
