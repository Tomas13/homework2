package com.example.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Reads raw bytes so the Confluent wire format can be inspected:
 * byte 0 = magic byte (0), bytes 1-4 = schema id (big-endian int), rest = Avro binary.
 *
 * Optional arg: reader schema "v1" or "v2" to deserialize with a different schema than the
 * one used for writing (Avro schema resolution). Default "writer" = use the writer schema.
 */
public class AvroConsumer {

    public static void main(String[] args) {
        String readerVersion = args.length > 0 ? args[0] : "writer";
        Schema readerSchema = "writer".equals(readerVersion) ? null : Schemas.load(readerVersion);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Schemas.BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-demo-" + readerVersion);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        KafkaAvroDeserializer avro = new KafkaAvroDeserializer();
        avro.configure(Map.of("schema.registry.url", Schemas.REGISTRY), false);

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try { main.join(); } catch (InterruptedException ignored) { }
        }));

        System.out.println("Listening on topic '" + Schemas.TOPIC + "' (reader schema: " + readerVersion + "). Ctrl+C to stop.");
        try {
            consumer.subscribe(List.of(Schemas.TOPIC));
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, byte[]> rec : records) {
                    print(rec, avro, readerSchema);
                }
            }
        } catch (WakeupException ignored) {
            // shutting down
        } finally {
            consumer.close();
            avro.close();
        }
    }

    private static void print(ConsumerRecord<String, byte[]> rec, KafkaAvroDeserializer avro, Schema readerSchema) {
        byte[] bytes = rec.value();
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        byte magic = buf.get(0);
        int schemaId = buf.getInt(1);

        System.out.println("------------------------------------------------------------");
        System.out.printf("partition=%d offset=%d key=%s%n", rec.partition(), rec.offset(), rec.key());
        System.out.printf("payload bytes 0-4   : %s  -> magic=%d, schema id=%d%n", Schemas.hex(bytes, 5), magic, schemaId);

        GenericRecord value = (GenericRecord) avro.deserialize(rec.topic(), bytes);
        System.out.println("writer schema fields: " + value.getSchema().getFields().stream().map(Schema.Field::name).toList());
        System.out.println("deserialized (writer): " + value);

        if (readerSchema != null) {
            try {
                GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(value.getSchema(), readerSchema);
                BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, 5, bytes.length - 5, null);
                GenericRecord projected = reader.read(null, decoder);
                System.out.println("deserialized (reader): " + projected);
            } catch (Exception e) {
                System.out.println("could not resolve with reader schema: " + e.getMessage());
            }
        }
    }
}
