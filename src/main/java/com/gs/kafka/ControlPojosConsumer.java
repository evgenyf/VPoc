package com.gs.kafka;

import com.gs.kafka.pojo.ControlPojo;
import com.gs.kafka.pojo.EventPojo;
import com.gs.kafka.serializations.KafkaControlPojoDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Properties;

public class ControlPojosConsumer extends AbstractConsumer {

    private SpaceDataWriter spaceDataWriter;

    public ControlPojosConsumer(String topic, Properties kafkaProps ){
        super( topic, kafkaProps, KafkaControlPojoDeserializer.class.getName() );
        spaceDataWriter = new SpaceDataWriter();
    }

    @Override
    protected void readFromKafka() {
        ConsumerRecords<String, ControlPojo> records = kafkaConsumer.poll(Duration.ofMillis(100));
        int index = 0;
        ControlPojo[] pojos = new ControlPojo[ records.count() ];

        for (ConsumerRecord<String, ControlPojo> record : records) {
            ControlPojo pojo = record.value();
            //System.out.println("CONTROL, Message received, key:" + record.key() + ", value:" + pojo);
            pojos[ index++ ] = pojo;
        }
        if( pojos.length > 0 ) {
            //System.out.println("before write to space, " + pojos.length);
            spaceDataWriter.writeToSpace(pojos);
            //System.out.println("After write to space, " + pojos.length);
        }
    }
}