package com.example.avro;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class AvroProducer {

    public static void main(String[] args) throws Exception {
        String version = args.length > 0 ? args[0] : "v1";
        Schema schema = Schemas.load(version);

        GenericRecord record = new GenericData.Record(schema);
        record.put("id", UUID.randomUUID().toString());
        record.put("text", "Hello Kafka from Avro " + version);
        record.put("timestamp", System.currentTimeMillis());
        if (schema.getField("sender") != null) {
            record.put("sender", "java-producer");
            record.put("priority", 5);
        }

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Schemas.BOOTSTRAP);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", Schemas.REGISTRY);
        props.put("auto.register.schemas", true);

        // Serialize once ourselves just to show what goes on the wire (same serializer, same schema id).
        try (KafkaAvroSerializer preview = new KafkaAvroSerializer()) {
            preview.configure(Map.of("schema.registry.url", Schemas.REGISTRY), false);
            byte[] bytes = preview.serialize(Schemas.TOPIC, record);
            int schemaId = ByteBuffer.wrap(bytes).getInt(1);
            System.out.println("Schema version file : message-" + version + ".avsc");
            System.out.println("Record              : " + record);
            System.out.println("Payload first bytes : " + Schemas.hex(bytes, 5) + "  (magic byte + 4-byte schema id)");
            System.out.println("Registered schema id: " + schemaId);
        }

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            RecordMetadata meta = producer
                    .send(new ProducerRecord<>(Schemas.TOPIC, record.get("id").toString(), record))
                    .get();
            System.out.printf("Sent to %s partition=%d offset=%d%n", meta.topic(), meta.partition(), meta.offset());
        }
    }
}
