package com.example.streams.serialize;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.kstream.Windowed;

import java.nio.charset.StandardCharsets;
import java.util.Map;


/**
 */
public class WindowedString implements Serde<Windowed<String>> {

    @Override
    public void configure(Map<java.lang.String, ?> map, boolean b) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<Windowed<String>> serializer() {
        return new Serializer<Windowed<String>>() {

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String s, Windowed<String> data) {
                if (data == null) {
                    return null;
                }

                return data.key().getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public void close() {
            }
        };

    }

    @Override
    public Deserializer<Windowed<String>> deserializer() {
        return new Deserializer<Windowed<String>>() {

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public Windowed<String> deserialize(String topic, byte[] data) {
                throw new StreamsException("WindowedString can not be deserialized");
            }

            @Override
            public void close() {
            }
        };
    }
}
