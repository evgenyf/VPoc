package com.gs.kafka;

import com.gs.kafka.pojo.ControlPojo;
import com.gs.kafka.serializations.KafkaControlPojoSerializer;
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


public class ControlPojosProducer {

    private final ControlPojo[] pojos;
    //private final Random rand;
    private final String topic;
    private final int numberOfObjectsPerBatch;
    private final int writePeriodInSeconds;


    private final Producer<String, ControlPojo> kafkaProducer;

    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    //private final static int MAX_POSSIBLE_C_VAL = 100;
    private final static int DEFAULT_TTL_VAL =  20;

    private ScheduledFuture<?> future;
    private boolean order = true;

    public ControlPojosProducer( String topic, Properties kafkaProps, int numberOfObjectsPerBatch, int writePeriodInSeconds ){

        this.topic = topic;
        this.writePeriodInSeconds = writePeriodInSeconds;
        this.numberOfObjectsPerBatch = numberOfObjectsPerBatch;

        this.pojos = new ControlPojo[ numberOfObjectsPerBatch ];

        createAndFillInitialArray();

        this.kafkaProducer = new KafkaProducer(initKafkaProducerProperties(kafkaProps));

        scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1);
    }

    public void startWritingToKafka(){
        future = scheduledThreadPoolExecutor.scheduleAtFixedRate(() -> {
            updateArray( order );
            order = !order;
            System.out.println( "START WRITTING Control POjo BATCH" );
            long startTime = System.currentTimeMillis();
            for( int i = 0; i < numberOfObjectsPerBatch; i++  ) {
                sendToKafka("myKey", pojos[i]);
            }
            System.out.println( "STOP WRITTING Control POjo  BATCH, writing of [" + numberOfObjectsPerBatch +
                    "] took " + ( System.currentTimeMillis() - startTime ) + " msec." );
        }, 0, writePeriodInSeconds, TimeUnit.SECONDS);
    }

    public void cancelWritingToKafka(){
        future.cancel( true );
    }

    private Properties initKafkaProducerProperties(Properties kafkaProps){
        Map<Object,Object> props = new HashMap(kafkaProps);
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "1");
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaControlPojoSerializer.class.getName());
        return Utils.toProperties(props);
    }

    private void createAndFillInitialArray() {
        for( int i = 0; i < pojos.length; i++ ){
            pojos[ i ] = new ControlPojo( "A" + i, "B" + 1, DEFAULT_TTL_VAL );
        }
    }

    private void updateArray( boolean order ) {
        if( order ) {
            for (int i = 0; i < pojos.length; i++) {
                setCValue( i );
            }
        }
        else{
            for( int i = pojos.length - 1; i >=0 ; i-- ){
                setCValue( i );
            }
        }
    }

    private void setCValue( int i ){
        pojos[ i ].setC( "C" + i );
    }

    private void sendToKafka( String key, ControlPojo value ){
        try {
            ProducerRecord<String, ControlPojo> producerRecord = new ProducerRecord<>( topic, key, value );
            Future<RecordMetadata> future = kafkaProducer.send( producerRecord );
            //RecordMetadata recordMetadata = future.get(KAFKA_TIMEOUT, TimeUnit.SECONDS);
            //System.out.println("Written message to Kafka, key:" + producerRecord.key() + ", value=" + producerRecord.value() + ". partition: " + recordMetadata.partition() + ", offset: " + recordMetadata.offset());
        } catch (Exception e) {
            throw new KafkaException("Failed to write to kafka", e);
        }
    }
}