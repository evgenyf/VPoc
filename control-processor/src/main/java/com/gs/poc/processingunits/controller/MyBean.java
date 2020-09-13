/*
 * Copyright (c) 2008-2016, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gs.poc.processingunits.controller;

import com.gigaspaces.query.ISpaceQuery;
import com.gs.poc.kafka.pojo.ControlPojo;
import com.gs.poc.kafka.pojo.EventPojo;
import com.gs.poc.kafka.serializations.KafkaControlPojoDeserializer;
import com.gs.poc.kafka.serializations.KafkaEventPojoDeserializer;
import com.gs.poc.processingunits.controller.utils.KafkaConsumerUtils;
import com.j_spaces.core.client.SQLQuery;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.openspaces.core.GigaSpace;
import org.openspaces.core.cluster.ClusterInfo;
import org.openspaces.core.cluster.ClusterInfoContext;
import org.openspaces.core.space.status.SpaceStatusChanged;
import org.openspaces.core.space.status.SpaceStatusChangedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Properties;

public class MyBean {
    private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    @Resource // Injected by Spring
    private GigaSpace gigaSpace;

    @ClusterInfoContext //Injected by GigaSpaces
    private ClusterInfo clusterInfo;

    @Value("${space.name}") // Injected by Spring
    private String spaceName;

    private String id;

    protected Consumer<String,ControlPojo> controlPojosKafkaConsumer;
    protected Consumer<String,EventPojo> eventPojosKafkaConsumer;

    private boolean readFromControlTopic = false;
    private boolean readFromEventsTopic = false;

    private static String CONTROL_TOPIC = "control";
    private static String EVENTS_TOPIC = "events";

    @PostConstruct
    public void initialize() {
        id = gigaSpace.getSpaceName() + "[" + (clusterInfo != null ? clusterInfo.getSuffix() : "non-clustered") + "]";
        logger.info("Initialized {}", id);
        // NOTE: This method is called for both primary and backup instances.
        // If you wish to do something for primaries only, see @SpaceStatusChanged
    }

    private void initKafkaConsumers(){

        Properties kafkaProps = new Properties();
        kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + 9092);

        this.controlPojosKafkaConsumer = KafkaConsumerUtils.createConsumer( kafkaProps, KafkaControlPojoDeserializer.class, CONTROL_TOPIC );
        this.eventPojosKafkaConsumer = KafkaConsumerUtils.createConsumer( kafkaProps, KafkaEventPojoDeserializer.class, EVENTS_TOPIC );
    }

    @SpaceStatusChanged
    public void onSpaceStatusChange(SpaceStatusChangedEvent event) {
        logger.info("Space {} is {}", id, event.getSpaceMode());
        ControlPojo controlPojo = new ControlPojo();
        if (event.isActive()) {
            logger.info("Space {} is {} ACTIVE !!!!", id, event.getSpaceMode());
            initKafkaConsumers();
            startReadingFromKafka();
        } else {
            // Space is backup, or space is primary but suspended.
            // If your code should only run when the space is active, you should deactivate it here.
        }
    }

    @PreDestroy
    public void close() {
        logger.info("Closing {}", id);
    }

    private void startReadingFromKafka(){
        new Thread(() -> {
            readFromControlTopic = true;
            while (readFromControlTopic) {
                readAndHandleEventsFromControlTopic();
            }
        }).start();

        new Thread(() -> {
            readFromEventsTopic = true;
            while (readFromEventsTopic) {
                readAndHandleEventsFromEventsTopic();
            }
        }).start();
    }

    private void readAndHandleEventsFromControlTopic(){
        ConsumerRecords<String, ControlPojo> records = controlPojosKafkaConsumer.poll(Duration.ofMillis(100));
        int index = 0;
        ControlPojo[] pojos = new ControlPojo[ records.count() ];

        for (ConsumerRecord<String, ControlPojo> record : records) {
            ControlPojo controlPojo = record.value();
            //System.out.println("CONTROL, Message received, key:" + record.key() + ", value:" + pojo);
            pojos[ index++ ] = controlPojo;
        }
        if( pojos.length > 0 ) {
            //System.out.println("before write to space, " + pojos.length);

            gigaSpace.writeMultiple(pojos);//, ttl*1000 );

            //System.out.println("After write to space, " + pojos.length);
        }
    }

    private void readAndHandleEventsFromEventsTopic(){
        ConsumerRecords<String, EventPojo> records = eventPojosKafkaConsumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, EventPojo> record : records) {
            EventPojo eventPojo = record.value();

            ISpaceQuery<ControlPojo> controlPojoQuery = createControlQuery( eventPojo );
            if( controlPojoQuery != null ) {
                ControlPojo controlPojo = gigaSpace.read(controlPojoQuery);
                logger.info( "Query {} , Pojo {}", controlPojoQuery, controlPojo );
                if( controlPojo != null ){
                    String a = controlPojo.getA();
                    String b = controlPojo.getB();

                    final String d = a + b;
                    eventPojo.setD( d );
                    //write to enriched topic "enriched"

                }
            }
        }
    }

    private ISpaceQuery<ControlPojo> createControlQuery( EventPojo eventPojo ){
        ISpaceQuery<ControlPojo> query = null;
        if( eventPojo.getA() != null){
            query = new SQLQuery<>(ControlPojo.class, "a='" + eventPojo.getA() + "'");
        }
        else if( eventPojo.getB() != null ){
            query = new SQLQuery<>(ControlPojo.class, "b='" + eventPojo.getB() + "'" );
        }
        else if( eventPojo.getC() != null ){
            query = new SQLQuery<>(ControlPojo.class, "c='" + eventPojo.getC() + "'" );
        }

        return query;
    }

    public void stopReadingFromEventsKafkaTopic(){
        readFromEventsTopic = false;
    }

    public void stopReadingFromControlKafkaTopic(){
        readFromControlTopic = false;
    }
}