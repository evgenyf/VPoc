package com.gs.poc.kafka.serializations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gs.poc.kafka.pojo.EventPojo;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaEventPojoDeserializer implements Deserializer<EventPojo> {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public EventPojo deserialize(String topic, byte[] data) {
        //return (EventPojo) SerializationUtils.deserialize(data);
        return deserializeObject(data);
    }

    @Override
    public EventPojo deserialize(String topic, Headers headers, byte[] data) {
        return deserializeObject(data);
    }

    private EventPojo deserializeObject(byte[] data) {
        //return (EventPojo) SerializationUtils.deserialize(data);
        EventPojo eventPojo;
        try {
            eventPojo = objectMapper.readValue(data, EventPojo.class);
        } catch (Exception e) {
            throw new SerializationException("Error while deserializing", e);
        }
        return eventPojo;
    }
}