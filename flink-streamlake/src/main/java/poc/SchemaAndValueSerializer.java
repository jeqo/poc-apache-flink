package poc;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

public class SchemaAndValueSerializer implements Serializer<SchemaAndValue> {

    final Converter converter;

    public SchemaAndValueSerializer(Converter converter) {
        this.converter = converter;
    }

    @Override
    public byte[] serialize(String s, SchemaAndValue schemaAndValue) {
        return converter.fromConnectData(s, schemaAndValue.schema(), schemaAndValue.value());
    }
}
