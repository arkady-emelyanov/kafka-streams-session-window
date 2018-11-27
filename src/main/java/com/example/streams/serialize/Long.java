package com.example.streams.serialize;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.errors.StreamsException;

import java.nio.charset.StandardCharsets;
import java.util.Map;


/**
 */
public class Long implements Serde<java.lang.Long> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<java.lang.Long> serializer() {
        return new Serializer<java.lang.Long>() {

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public byte[] serialize(String s, java.lang.Long data) {
                if (data == null) {
                    return null;
                }

                return data.toString().getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public void close() {
            }
        };

    }

    @Override
    public Deserializer<java.lang.Long> deserializer() {
        return new Deserializer<java.lang.Long>() {

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
            }

            @Override
            public java.lang.Long deserialize(String topic, byte[] data) {
                throw new StreamsException("Long can not be deserialized");
            }

            @Override
            public void close() {
            }
        };
    }
}
