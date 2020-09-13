package com.gs.poc.processingunits.controller.utils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.stream.Collectors;

public class KafkaConsumerUtils {

    public static Consumer createConsumer( Properties kafkaProps, Class valueDeserializerClass, String topic ){
        KafkaConsumer kafkaConsumer = new KafkaConsumer(KafkaConsumerUtils.initConsumerProperties(kafkaProps, valueDeserializerClass ));
        kafkaConsumer.assign(KafkaConsumerUtils.initTopicPartitions( kafkaConsumer, topic ));
        return kafkaConsumer;
    }

    private static Set<TopicPartition> initTopicPartitions(Consumer consumer, String topic ){
        List<PartitionInfo> partitionInfos;
        while (true){
            try{
                partitionInfos = consumer.partitionsFor(topic);
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

    private static Properties initConsumerProperties(Properties kafkaProps, Class valueDeserializerClass ){
        Map<Object,Object> props = new HashMap<>(kafkaProps);
        props.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass.getName());
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        return com.gs.poc.kafka.utils.Utils.toProperties(props);
    }
}