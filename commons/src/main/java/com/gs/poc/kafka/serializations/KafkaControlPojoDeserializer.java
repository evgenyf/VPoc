package com.gs.poc.kafka.serializations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gs.poc.kafka.pojo.ControlPojo;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaControlPojoDeserializer implements Deserializer<ControlPojo> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public ControlPojo deserialize(String topic, byte[] data) {
        return deserializeObject(data);
    }

    @Override
    public ControlPojo deserialize(String topic, Headers headers, byte[] data) {
        return deserializeObject(data);
    }

    private ControlPojo deserializeObject(byte[] data) {
        ControlPojo controlPojo;
        try {
            controlPojo = objectMapper.readValue(data, ControlPojo.class);
        } catch (Exception e) {
            throw new SerializationException("Error while deserializing", e);
        }
        return controlPojo;
    }
}