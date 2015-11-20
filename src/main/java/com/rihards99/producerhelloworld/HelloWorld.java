package com.rihards99.producerhelloworld;

import java.util.Date;
import java.util.Properties;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class HelloWorld {

    public static final String BROKER_ADDR = "sandbox.hortonworks.com:6667";
    public static final String TOPIC = "test";
    
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("metadata.broker.list", BROKER_ADDR); // List of brokers that the producer asks to get the topic leader
        props.put("serializer.class", "kafka.serializer.StringEncoder"); // Message encoder class
        props.put("partitioner.class", "com.mycompany.logproducer.SimplePartitioner"); // Partitioning logic
        props.put("request.required.acks", "1"); // Producer asks for confirmation from broker that the message was recieved
        ProducerConfig config = new ProducerConfig(props);
        
        // The two types for the Producer generic are partition key type and message type
        Producer<String, String> producer = new Producer<String, String>(config);
        
        // Generate the message
        long runtime = new Date().getTime();
        String msg = "Hello World! (" + runtime + ")";
        
        // Send the message
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, msg);
        producer.send(data);
    }
}
