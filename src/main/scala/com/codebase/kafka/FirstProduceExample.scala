package com.codebase.kafka
import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord


/**
  * Created by samarapa on 2/27/2017.
  */
object FirstProduceExample {
  def main(args: Array[String]) {
    val brokers = args(2);
    val props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("client.id", "ProducerExample");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    val producer = new KafkaProducer[String,String](props);
    val topic = "test";
    for (i <- 1 to 50){
      val record = new ProducerRecord(topic, "key",s"hello $i")
      producer.send(record);
    }
    val record = new ProducerRecord(topic, "key",s"End of message")
    producer.send(record);
    producer.close();
  }

}
