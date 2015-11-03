/**
 * Copyright (C) 2014-2015 SINTEF
 *
 *     Brian Elvesæter <brian.elvesater@sintef.no>
 *     Shahzad Karamat <shazad.karamat@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.modelbased.proasense.adapter.base;

import eu.proasense.internal.ComplexValue;
import eu.proasense.internal.SimpleEvent;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import java.util.Map;
import java.util.Properties;


public class KafkaProducerOutput {
    private String bootstrapServers;
    private String topic;
    private String sensorId;
    private Boolean publish;
//    private KafkaProducer<String, byte[]> producer;
    private Producer<String, byte[]> producer;


	public KafkaProducerOutput(String bootstrapServers, String topic, String sensorId, Boolean publish) {
        // Initialize properties
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.sensorId = sensorId;
        this.publish = publish;

        // Define the producer object
        this.producer = createOldProducer(this.bootstrapServers);
    }


    private KafkaProducer<String, byte[]> createNewProducer(String bootstrapServers) {
        // Specify producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        // Define the producer object
        org.apache.kafka.clients.producer.KafkaProducer<String, byte[]> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, byte[]>(props);

        return producer;
    }


    private Producer<String, byte[]> createOldProducer(String bootstrapServers) {
        // Specify producer properties
        Properties props = new Properties();
        props.put("metadata.broker.list", bootstrapServers);
        props.put("producer.type", "sync");
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");

        ProducerConfig config = new ProducerConfig(props);

        // Define the producer object
        Producer<String, byte[]> producer = new Producer<String, byte[]>(config);

        return producer;
    }


    public void publishSimpleEvent(SimpleEvent event) {
        this.publishSimpleEvent(event, this.topic);
    }


    public void publishSimpleEvent(SimpleEvent event, String topic) {
        try {
            // Serialize message
            TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
            byte[] bytes = serializer.serialize(event);

            // Publish message
            if (this.publish) {
//                ProducerRecord<String, byte[]> message = new ProducerRecord<String, byte[]>(topic, "adapterkey", bytes);
                KeyedMessage<String, byte[]> message = new KeyedMessage<String, byte[]>(topic, "adapterkey", bytes);
                this.producer.send(message);
            }
        }
        catch (TException e) {
            System.out.println(e.getClass().getName() + ": " + e.getMessage());
        }
    }


    public SimpleEvent createSimpleEvent(long timestamp, Map<String, ComplexValue> eventProperties) {
        return createSimpleEvent(this.sensorId, timestamp, eventProperties);
    }


    public SimpleEvent createSimpleEvent(String sensorId, long timestamp, Map<String, ComplexValue> eventProperties) {
        SimpleEvent event = new SimpleEvent();

        event.setSensorId(sensorId);
        event.setTimestamp(timestamp);
        event.setEventProperties(eventProperties);

        return event;
    }


    public void close() {
        this.producer.close();
    }
}
