package poc;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

public class SchemaAndValueSerde implements Serde<SchemaAndValue> {

    final Converter converter;

    public SchemaAndValueSerde(Converter converter) {
        this.converter = converter;
    }

    @Override
    public Serializer<SchemaAndValue> serializer() {
        return new SchemaAndValueSerializer(converter);
    }

    @Override
    public Deserializer<SchemaAndValue> deserializer() {
        return new SchemaAndValueDeserializer(converter);
    }
}
