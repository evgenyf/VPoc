package com.gs.poc.kafka.serializations;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gs.poc.kafka.pojo.ControlPojo;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaControlPojoSerializer implements Serializer<ControlPojo> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, ControlPojo data) {
        return serializeObject(data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, ControlPojo data) {
        return serializeObject(data);
    }

    private byte[] serializeObject(ControlPojo data) {
        //return SerializationUtils.serialize(data);
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {

    }
}