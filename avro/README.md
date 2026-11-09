# Kafka + Avro (Schema Registry)

A Java Kafka producer serializes a `Message` record with Avro (Confluent `KafkaAvroSerializer`). A Java consumer reads the raw bytes, decodes the Confluent wire-format header, deserializes the record, and prints it. There are two schema versions, so you can watch the schema id change.

```
docker-compose.yml                 Kafka 7.7.1 (KRaft, single node) + Schema Registry on :8081
src/main/resources/avro/
  message-v1.avsc                  id, text, timestamp
  message-v2.avsc                  + sender (default "unknown"), priority (default 0)
src/main/java/com/example/avro/
  AvroProducer.java                sends one GenericRecord using schema v1 or v2
  AvroConsumer.java                prints header bytes, schema id, and the record
scripts/registry.sh                shows subjects / versions / ids in Schema Registry
```

## Wire format (value bytes)
| byte  | meaning |
|-------|---------|
| 0     | magic byte, always `0x00` |
| 1–4   | schema id (big-endian int) from Schema Registry |
| 5…    | Avro binary data (no schema inside) |

## Run
Prerequisites: Java 17, plus Docker via Colima (`colima start`).

```bash
cd avro
docker compose up -d                                    # Kafka :9092, Schema Registry :8081
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic avro-messages --partitions 1 --replication-factor 1

./gradlew runConsumer                 # terminal 1: reads with the writer's schema
./gradlew runProducer -Pargs=v1       # terminal 2: send a message with schema v1
./gradlew runProducer -Pargs=v2       # send a message with schema v2
./scripts/registry.sh                 # see versions 1, 2 and their ids

# deserialize with a different (reader) schema than the writer used:
./gradlew runConsumer -Pargs=v1       # v2 messages lose sender/priority
./gradlew runConsumer -Pargs=v2       # v1 messages get the defaults
```
Each consumer variant uses its own consumer group (`avro-demo-<arg>`) and starts at the earliest offset the first time it runs. After that it resumes from where it stopped, so on later runs it only shows new messages.

Inspect the raw bytes on the topic. Each message starts with `00` followed by the 4-byte schema id:
```bash
docker compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic avro-messages --from-beginning --timeout-ms 5000 | xxd | head
```

### Stop / restart
- Pause, keeping the data: `colima stop`. Resume with `colima start && docker compose up -d`.
- Remove everything: `docker compose down`, then `colima stop`. This deletes the topic and the registered schemas, so you have to create the topic again, and schema ids start at 1 again.

## Observed output

Producer:
```
Schema version file : message-v1.avsc
Payload first bytes : 00 00 00 00 01  (magic byte + 4-byte schema id)
Registered schema id: 1
Sent to avro-messages partition=0 offset=0

Schema version file : message-v2.avsc
Payload first bytes : 00 00 00 00 02  (magic byte + 4-byte schema id)
Registered schema id: 2
Sent to avro-messages partition=0 offset=1
```

Consumer (writer schema):
```
partition=0 offset=0 key=182f9f75-6406-4670-a8ce-551783eea09d
payload bytes 0-4   : 00 00 00 00 01  -> magic=0, schema id=1
writer schema fields: [id, text, timestamp]
deserialized (writer): {"id": "182f9f75-...", "text": "Hello Kafka from Avro v1", "timestamp": 1794237722054}
------------------------------------------------------------
partition=0 offset=1 key=e5270bfb-8922-49cd-8bec-6be986d2e616
payload bytes 0-4   : 00 00 00 00 02  -> magic=0, schema id=2
writer schema fields: [id, text, timestamp, sender, priority]
deserialized (writer): {"id": "e5270bfb-...", "text": "Hello Kafka from Avro v2", "timestamp": 1794237725346, "sender": "java-producer", "priority": 5}
```

Different reader schema (Avro schema resolution):
```
reader=v1, v2 message:  {"id": "e5270bfb-...", "text": "Hello Kafka from Avro v2", "timestamp": 1794237725346}
reader=v2, v1 message:  {"id": "182f9f75-...", "text": "Hello Kafka from Avro v1", "timestamp": 1794237722054, "sender": "unknown", "priority": 0}
```

Schema Registry (`./scripts/registry.sh`):
```
Subjects: ["avro-messages-value"]
Versions of avro-messages-value: [1,2]
--- version 1
id: 1 | version: 1   fields: id, text, timestamp
--- version 2
id: 2 | version: 2   fields: id, text, timestamp, sender, priority
```

Raw bytes on the topic (`kafka-console-consumer ... | xxd`). Each message starts with `00` plus a 4-byte id:
```
00000000: 0000 0000 0148 3138 3266 3966 3735 2d36  .....H182f9f75-6     <- 00 | 00 00 00 01 (id 1)
...
00000040: 2076 318c b7b7 8eb8 680a 0000 0000 0248   v1.....h......H     <- 00 | 00 00 00 02 (id 2)
```

## Notes
- A schema id is global across the whole registry, while a version is counted per subject. Here the two happen to match (id 1 = v1, id 2 = v2) because the registry was empty when we started.
- v2 only adds fields with defaults, so it passes the registry's default `BACKWARD` compatibility check.
