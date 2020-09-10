package com.gs.poc.kafka.serializations;

import com.gs.poc.kafka.pojo.EventPojo;
import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaEventPojoDeserializer implements Deserializer<EventPojo> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public EventPojo deserialize(String topic, byte[] data) {
        return (EventPojo) SerializationUtils.deserialize(data);
    }

    @Override
    public EventPojo deserialize(String topic, Headers headers, byte[] data) {
        return (EventPojo) SerializationUtils.deserialize(data);
    }
}