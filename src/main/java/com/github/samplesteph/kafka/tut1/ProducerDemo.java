/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.github.samplesteph.kafka.tut1;


import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author armoshar
 */
public class ProducerDemo {
    public static void main(String[] args) {
        //System.out.println("Hello Arash");

        //create producer properties
        // if you go to Kafka documentation you can look up the properties
        String boostrapServer = "20.225.126.149:9092";
        Logger logger = LoggerFactory.getLogger(ProducerDemo.class);

        Properties  properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // create the producer 

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        //create a recond for producer
        for(int i =0 ;i <5; i++){
            String val = "Message " + i;

            // you use they to make sure same keys go to the same parition
            // for exmaple they key could be the truck id or something like that
            String key = "id_"+i; 
        ProducerRecord<String, String> record = new ProducerRecord<>("test_topic" ,key,val);
        // send data
       
        producer.send(record, new Callback(){
            public void onCompletion(RecordMetadata recordMetadata, Exception e){
               // executes everytime record is successfully sent
               if ( e ==null){
                   Timestamp stamp = new Timestamp(recordMetadata.timestamp());
                   Date date;
                   date = new Date(stamp.getTime());
                   logger.info("Received new metadata:\n "+ 
                           "Topic: "+ recordMetadata.topic()+ "\n"+
                           "Partition: "+ recordMetadata.partition()+"\n"+
                           "Offset: "+ recordMetadata.offset()+"\n"+
                           "Timestamp: "+date);
                
               }else{
                   logger.info("error: ",e);
               }
            }});
        }
        producer.flush();
        producer.close();
    }
}
