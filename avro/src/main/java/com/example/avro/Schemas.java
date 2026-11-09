package com.example.avro;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;

/** Loads the demo schemas from the classpath. */
public final class Schemas {
    public static final String TOPIC = "avro-messages";
    public static final String BOOTSTRAP = "localhost:9092";
    public static final String REGISTRY = "http://localhost:8081";

    private Schemas() {}

    public static Schema load(String version) {
        String path = "/avro/message-" + version + ".avsc";
        try (InputStream in = Schemas.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalArgumentException("Unknown schema version '" + version + "' (use v1 or v2)");
            }
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Hex dump of the first n bytes. */
    public static String hex(byte[] bytes, int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(n, bytes.length); i++) {
            sb.append(String.format("%02x ", bytes[i]));
        }
        return sb.toString().trim();
    }
}
