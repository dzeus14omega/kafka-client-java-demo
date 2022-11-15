package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;


import java.util.Properties;

public class KafkaProducerTest {
    public static void main(String[] args) {
        System.out.println("Hello , this is test for kafka client in java endPoint");

        //Creating properties
        System.out.println("Check1: start create kafka-client properties");
        String bootstrapServer="192.168.1.149:9092";  // replace this address with real kafka server address
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //creating producer
        System.out.println("Check2: creating new kafka producer");
        KafkaProducer<String,String> first_producer = new KafkaProducer<String, String>(properties);

        //creating producer record
        System.out.println("Check3: creating new kafka producer record");

        String message = "{ \n" +
                "    \"cam_id\": \"vhtits_001\",\n" +
                "    \"cam_name\": \"Cam nhận diện giao thông\",\n" +
                "    \"rtsp\": \" OJ2HG4B2F4XWY33DMFWGQ33TOQ5DQNJVGQXWE5LONZ4Q====\",\n" +
                "    \"address\": \"LLQ - Hà Nội\",\n" +
                "    \"lat\": \"20.8419428149783\",\n" +
                "    \"lng\": \"106.695325066884\", \n" +
                "    \"event_type\":\"Traffic\",\n" +
                "    \"metadata\":\n" +
                "    {}    \n}";

        ProducerRecord<String, String> record=new ProducerRecord<String, String>("iva.event.transport.json", message);

        //sending data
        System.out.println("Check4: start sending record");
        first_producer.send(record);

        //flush then close
        System.out.println("Check5: flush kafka producer");
        first_producer.flush();

        System.out.println("Check6: close kafka producer");
        first_producer.close();

        System.out.println("End! This test is finish here");
    }
}