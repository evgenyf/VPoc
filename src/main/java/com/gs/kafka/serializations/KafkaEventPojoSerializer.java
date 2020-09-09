package com.gs.kafka.serializations;

import com.gs.kafka.pojo.ControlPojo;
import com.gs.kafka.pojo.EventPojo;
import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaEventPojoSerializer implements Serializer<EventPojo> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, EventPojo data) {
        return SerializationUtils.serialize(data);
    }

    @Override
    public byte[] serialize(String topic, Headers headers, EventPojo data) {
        return SerializationUtils.serialize(data);
    }

    @Override
    public void close() {

    }
}