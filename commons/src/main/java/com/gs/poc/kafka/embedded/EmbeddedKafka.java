package com.gs.poc.kafka.embedded;

import com.gs.poc.kafka.utils.Utils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Alon Shoham
 */
public class EmbeddedKafka {
    private final Properties kafkaProperties;
    private final int kafkaPort;
    private final int zookeeperPort;
    private final File logDir;
    private KafkaServerStartable kafka;

    public EmbeddedKafka(int kafkaPort, int zookeeperPort) {
        this.kafkaPort = kafkaPort;
        this.zookeeperPort = zookeeperPort;
        this.logDir = Utils.tempDir("kafkaLogDir");
        this.kafkaProperties = initProperties();
    }

    public void startup() {
        startup(true);
    }

    public void startup(boolean deleteLogDir){
        if(deleteLogDir) {
            try {
                FileUtils.deleteDirectory(logDir);
            } catch (IOException e) {
                throw new RuntimeException("Failed to delete temp dirs", e);
            }
        }
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);
        kafka = new KafkaServerStartable(kafkaConfig);
        kafka.startup();
    }

    public void shutdown() {
        kafka.shutdown();
    }

    private Properties initProperties() {
        Properties props = new Properties();
        props.setProperty("zookeeper.connect", "localhost:" + zookeeperPort);
        props.setProperty("port", String.valueOf(kafkaPort));
        props.setProperty("broker.id", "0");
        props.setProperty("num.partitions", "1");
        props.setProperty("log.dirs", logDir.getAbsolutePath());
        props.setProperty("offsets.topic.replication.factor","1");
        return props;
    }
}
