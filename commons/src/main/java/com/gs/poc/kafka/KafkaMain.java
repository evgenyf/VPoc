package com.gs.poc.kafka;

import com.gs.poc.kafka.embedded.EmbeddedKafka;
import com.gs.poc.kafka.embedded.EmbeddedZookeeper;
import org.apache.kafka.clients.CommonClientConfigs;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

public class KafkaMain {

    static int i= 0;

    public static void main( String[] args ){

        String controlTopic = "control";
        String eventsTopic = "events";
/*
        List<Integer> ports;
        try {
            ports = Utils.choosePorts(2);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get ports due to" + e.getMessage(), e);
        }
        int zookeeperPort = ports.get(0);
        int kafkaPort = ports.get(1);*/

        int zookeeperPort = 2182;
        int kafkaPort = 9092;

        Properties kafkaProps = new Properties();
        kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaPort);

        EmbeddedZookeeper embeddedZookeeper = new EmbeddedZookeeper(zookeeperPort);
        try {
            embeddedZookeeper.startup();
        } catch (Exception e) {
            throw new RuntimeException( "Failed to startup embedded zookeeper due to " + e.getMessage(), e );
        }

        EmbeddedKafka embeddedKafka = new EmbeddedKafka(kafkaPort, zookeeperPort);
        embeddedKafka.startup();


        final int numberOfControlPojosPerBatch = 10_000;
        final int numberOfEventPojosPerBatch = 20_000;

        ControlPojosProducer controlPojosProducer = new ControlPojosProducer( controlTopic, kafkaProps, numberOfControlPojosPerBatch, 20 );
        controlPojosProducer.startWritingToKafka();

        EventPojosProducer eventPojosProducer = new EventPojosProducer( eventsTopic, kafkaProps, numberOfEventPojosPerBatch, numberOfControlPojosPerBatch, 1 );
        eventPojosProducer.startWritingToKafka();

/*        ControlPojosConsumer controlPojosConsumer = new ControlPojosConsumer( controlTopic, kafkaProps );
        controlPojosConsumer.startReadingFromKafka();
*/

/*        EventPojosConsumer eventPojosConsumer = new EventPojosConsumer( eventsTopic, kafkaProps );
        eventPojosConsumer.startReadingFromKafka();*/
    }
}