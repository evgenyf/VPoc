package com.gs.kafka;

import com.gs.kafka.pojo.EventPojo;
import com.gs.kafka.serializations.KafkaEventPojoDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Properties;

public class EventPojosConsumer extends AbstractConsumer{

    public EventPojosConsumer(String topic, Properties kafkaProps ){
        super(topic, kafkaProps, KafkaEventPojoDeserializer.class.getName());
    }

    @Override
    protected void readFromKafka() {
        ConsumerRecords<String, EventPojo> records = kafkaConsumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, EventPojo> record : records) {
            System.out.println("EVENT, Message received, key:" + record.key() + ", value:" + record.value());
        }
    }
}