package com.gs.poc.processingunits.controller.utils;

import com.gs.poc.kafka.serializations.KafkaEventPojoSerializer;
import com.gs.poc.kafka.utils.Utils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.stream.Collectors;

public class KafkaUtils {

    public static Consumer createConsumer( Properties kafkaProps, Class valueDeserializerClass, String topic ){
        KafkaConsumer kafkaConsumer = new KafkaConsumer(KafkaUtils.initConsumerProperties(kafkaProps, valueDeserializerClass ));
        kafkaConsumer.assign(KafkaUtils.initTopicPartitions( kafkaConsumer, topic ));
        return kafkaConsumer;
    }

    public static Producer createProducer(Properties kafkaProps, Class valueSerializerClass){
        return new KafkaProducer( initKafkaProducerProperties( kafkaProps, valueSerializerClass ) );
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

    private static Properties initKafkaProducerProperties(Properties kafkaProps, Class valueSerializerClass){
        Map<Object,Object> props = new HashMap<>(kafkaProps);
        props.putIfAbsent(ProducerConfig.ACKS_CONFIG, "1");
        props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getName());
        return Utils.toProperties(props);
    }
}