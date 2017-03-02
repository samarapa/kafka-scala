package com.codebase.kafka

import java.util.Properties
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

import org.apache.kafka.clients.consumer._

import collection.mutable._
;
/**
  * Created by samarapa on 3/2/2017.
  */
class ConsumerExample {

  val props = createConsumerConfig("192.168.0.31:9092","test");
  val consumer = new KafkaConsumer[String,String](props);

  def createConsumerConfig (brokers:String, groupId: String): Properties ={
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props
  }

  def run(): Unit =
  {
    val topic:java.util.List[String]  = ArrayBuffer("test");
    println("Start")
    consumer.subscribe(topic);
    println("Subscribition  Completed")
    while (true) {
      val records = consumer.poll(1000)

      for (record <- records) {
        System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
      }
    }
  }
  }

object Consumer
{
  def main(args: Array[String]) {
    val start = new ConsumerExample();
    start.run()
  }
}
