package com.gs.kafka;

import com.gs.kafka.pojo.EventPojo;
import com.gs.kafka.serializations.KafkaEventPojoSerializer;
import com.gs.kafka.utils.Utils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class EventPojosProducer {

    private final EventPojo[] pojos;
    private final Producer<String, EventPojo> kafkaProducer;

    private final Random rand;
    private final String topic;
    private final int numberOfObjectsPerBatch;
    private final int writePeriodInSeconds;
    private final int numberOfControlPojosPerBatch;

    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    private ScheduledFuture<?> future;

    public EventPojosProducer(String topic, Properties kafkaProps, int numberOfObjectsPerBatch, int numberOfControlPojosPerBatch, int writePeriodInSeconds ){

        this.topic = topic;
        this.writePeriodInSeconds = writePeriodInSeconds;
        this.numberOfObjectsPerBatch = numberOfObjectsPerBatch;
        this.numberOfControlPojosPerBatch = numberOfControlPojosPerBatch;

        this.pojos = new EventPojo[ numberOfObjectsPerBatch ];

        createInitialArray();

        this.kafkaProducer = new KafkaProducer(initKafkaProducerProperties(kafkaProps));
        this.rand = new Random();

        scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
    }

    public void startWritingToKafka(){
        future = scheduledThreadPoolExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                updateArray();
                System.out.println( "START WRITTING Event Pojo BATCH" );
                long startTime = System.currentTimeMillis();
                for( int i = 0; i < numberOfObjectsPerBatch; i++  ) {
                    sendToKafka("myKey", pojos[i]);
                }
                System.out.println( "STOP WRITTING Event POjo BATCH, writing of [" + numberOfObjectsPerBatch + "] took " + ( System.currentTimeMillis() - startTime ) + " msec." );
            }
        }, 0, writePeriodInSeconds, TimeUnit.SECONDS);
    }

    public void cancelWritingToKafka(){
        future.cancel( true );
    }

    private Properties initKafkaProducerProperties(Properties kafkaProps){
        Map<Object,Object> props = new HashMap(kafkaProps);
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "1");
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaEventPojoSerializer.class.getName());
        return Utils.toProperties(props);
    }

    private void createInitialArray() {
        for( int i = 0; i < pojos.length; i++ ){
            pojos[ i ] = new EventPojo();
        }
    }

    private void updateArray() {
        for( int i = 0; i < pojos.length; i++ ){
            int randomInt = rand.nextInt(3);
            EventPojo pojo = pojos[i];
            int index = i >= numberOfControlPojosPerBatch ? numberOfControlPojosPerBatch - i : i;
            switch (randomInt){
                case 0:
                    pojo.setA( "A" + index );
                    pojo.setB( null );
                    pojo.setC( null );
                    break;
                case 1:
                    pojo.setA( null );
                    pojo.setB( "B" + index );
                    pojo.setC( null );
                        break;
                case 2:
                    pojo.setA( null );
                    pojo.setB( null );
                    pojo.setC( "C" + index );
                    break;
            }
        }
    }

    private void sendToKafka( String key, EventPojo value ){
        try {
            ProducerRecord<String, EventPojo> producerRecord = new ProducerRecord<>( topic, key, value );
            Future<RecordMetadata> future = kafkaProducer.send( producerRecord );
            //RecordMetadata recordMetadata = future.get(KAFKA_TIMEOUT, TimeUnit.SECONDS);
            //System.out.println("Written message to Kafka, key:" + producerRecord.key() + ", value=" + producerRecord.value() + ". partition: " + recordMetadata.partition() + ", offset: " + recordMetadata.offset());
        } catch (Exception e) {
            throw new KafkaException("Failed to write to kafka", e);
        }
    }
}