package com.gs.poc.kafka.embedded;

import com.gs.poc.kafka.utils.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * @author Alon Shoham
 */
public class EmbeddedZookeeper {

    private final int port;

    private NIOServerCnxnFactory factory;
    private ZooKeeperServer zooKeeper;

    private File snapDir;
    private File logDir;

    public EmbeddedZookeeper(int port) {
        this.port = port;
    }

    public void startup() throws IOException, InterruptedException {
        snapDir = Utils.tempDir("zookeeperSnapDir");
        logDir = Utils.tempDir("zookeeperLogDir");
        int tickTime = 2000;

        try {
            FileUtils.deleteDirectory(logDir);
            FileUtils.deleteDirectory(snapDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete temp dirs", e);
        }

        zooKeeper = new ZooKeeperServer(snapDir, logDir, tickTime);
        factory = new NIOServerCnxnFactory();
        factory.configure(new InetSocketAddress(port), 0);
        factory.startup(zooKeeper);
    }

    public void shutdown() {
        zooKeeper.shutdown();
        factory.shutdown();
    }
}
