package com.gs.kafka.serializations;

import com.gs.kafka.pojo.ControlPojo;
import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaControlPojoDeserializer implements Deserializer<ControlPojo> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public ControlPojo deserialize(String topic, byte[] data) {
        return (ControlPojo) SerializationUtils.deserialize(data);
    }

    @Override
    public ControlPojo deserialize(String topic, Headers headers, byte[] data) {
        return (ControlPojo) SerializationUtils.deserialize(data);
    }
}