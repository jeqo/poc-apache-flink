package poc;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

public class SchemaAndValueDeserializer implements Deserializer<SchemaAndValue> {

    final Converter converter;

    public SchemaAndValueDeserializer(Converter converter) {
        this.converter = converter;
    }

    @Override
    public SchemaAndValue deserialize(String s, byte[] bytes) {
        return converter.toConnectData(s, bytes);
    }
}
