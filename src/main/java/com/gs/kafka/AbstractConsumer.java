package com.gs.kafka;

import com.gs.kafka.utils.Utils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.stream.Collectors;

abstract public class AbstractConsumer {

    private final String topic;
    protected final Consumer kafkaConsumer;
    private boolean read = false;
    private final String deserializerValueClassName;

    public AbstractConsumer(String topic, Properties kafkaProps, String deserializerValueClassName ){

        this.topic = topic;
        this.deserializerValueClassName = deserializerValueClassName;
        Properties consumerProperties = initConsumerProperties(kafkaProps);

        this.kafkaConsumer = new KafkaConsumer(consumerProperties);

        Set<TopicPartition> topicPartitions = initTopicPartitions();
        kafkaConsumer.assign(topicPartitions);
    }

    private Properties initConsumerProperties(Properties kafkaProps){
        Map<Object,Object> props = new HashMap(kafkaProps);
        props.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializerValueClassName /*KafkaEventPojoDeserializer.class.getName()*/);
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return Utils.toProperties(props);
    }

    private Set<TopicPartition> initTopicPartitions(){
        List<PartitionInfo> partitionInfos;
        while (true){
            try{
                partitionInfos = kafkaConsumer.partitionsFor(topic);
                if(partitionInfos != null) {
                    return partitionInfos.stream().map(p -> new TopicPartition(p.topic(), p.partition())).collect(Collectors.toSet());
                }
            } catch (RuntimeException e){
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException interruptedException) {
                    throw new RuntimeException("Interrupted while getting kafka partitions for topic " + topic);
                }
            }
        }
    }

    public void startReadingFromKafka(){
        new Thread(() -> {
            read = true;
            while (read) {
                readFromKafka();
            }
        }).start();
    }

    abstract protected void readFromKafka();

    public void stopReadingFromKafka(){
        read = false;
    }
}