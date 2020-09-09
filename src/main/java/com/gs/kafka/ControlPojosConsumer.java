package com.gs.kafka;

import com.gs.kafka.pojo.EventPojo;
import com.gs.kafka.serializations.KafkaControlPojoDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Properties;

public class ControlPojosConsumer extends AbstractConsumer {

    public ControlPojosConsumer(String topic, Properties kafkaProps ){
        super( topic, kafkaProps, KafkaControlPojoDeserializer.class.getName() );
    }

    @Override
    protected void readFromKafka() {
        ConsumerRecords<String, EventPojo> records = kafkaConsumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, EventPojo> record : records) {
            System.out.println("CONTROL, Message received, key:" + record.key() + ", value:" + record.value());
        }
    }
}