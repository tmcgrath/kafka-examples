package com.supergloo

import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{CommitFailedException, ConsumerConfig, KafkaConsumer}

object KafkaConsumerExample {

  import scala.collection.JavaConverters._

  val bootstrapServers = ":9092,localhost:9092"
  val groupId = "kafka-example"
  val topics = "example-topic"

  val props: Properties = {
    val p = new Properties()
    p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    // if we want to adjust defaults
     p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // default is latest
    // p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false) // default 5000 - change how often to commit offsets
    // p.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000) // default 5000 - change how often to commit offsets
    p
  }

  def main(args: Array[String]): Unit = {

    val consumer = new KafkaConsumer[String, String](props)

    consumer.subscribe(Collections.singletonList(this.topics))

    while (true) {
      val records = consumer.poll(1000).asScala

      for (record <- records) {
        println("Message: (key: " +
          record.key() + ", with value: " + record.value() +
          ") at on partition " + record.partition() + " at offset " + record.offset())
      }

      // if you don't want to auto commit offset which is default
      // comment out below and adjust properties above
      /*
      try
        consumer.commitSync
      catch {
        case e: CommitFailedException =>
         // possible rollback of processed records
         // which may have failed to be delivered to ultimate destination
      }
      */
    }


    consumer.close()
  }
}
