package com.codebase.kafka
import java.util.Properties

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord


/**
  * Created by samarapa on 2/27/2017.
  */
object FirstProducerExample {
  def main(args: Array[String]) {
    val brokers = "192.168.0.31:9092"
    val props = new Properties();
    props.put("bootstrap.servers", brokers);
    props.put("client.id", "ProducerExample");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    println("Kafka Produce Start")
    val producer = new KafkaProducer[String, String](props);
    //println("Kafka Produce ended")
    val topic = "test";
    try {
      println("Start sending messages...!")
      for (i <- 1 to 5) {
        val record = new ProducerRecord(topic, "key", s"hello $i")
         producer.send(record);
            }
      val record = new ProducerRecord(topic, "key", s"End of message")
      producer.send(record);
      println("Finish sending messages...!")
      producer.close();
      println("Producer connection Closed...!")
       } catch{
      case e: Exception => { e.printStackTrace(); e.toString() }
    }
  }

}
