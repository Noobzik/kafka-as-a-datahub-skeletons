package org.esgi.project.kafka

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.streams.StreamsConfig
import org.esgi.project.kafka.models.ConnectionEvent

import java.util.Properties
import java.util.concurrent.{ScheduledFuture, TimeUnit}

object MessageProcessing extends PlayJsonSupport with SimpleSchedulingLoops {
  val props = buildProperties

  // TODO: fill your first name & last name
  val yourFirstName: String = ???
  val yourLastName: String = ???

  val applicationName = s"simple-app-$yourFirstName-$yourLastName"
  val topicName: String = "connection-events"

  def run(): ScheduledFuture[_] = {
    producerScheduler.schedule(producerLoop, 1, TimeUnit.SECONDS)
    consumerScheduler.schedule(consumerLoop, 1, TimeUnit.SECONDS)
  }

  // TODO: implement message production
  def producerLoop = {
    // Instantiating the producer
    // toSerializer comes from PlayJsonSupport and implements Serdes automatically from Play Json directives
    val producer = new KafkaProducer[String, ConnectionEvent](props, toSerializer[String], toSerializer[ConnectionEvent])

    // TODO: use this loop to produce messages
    while (!producerScheduler.isShutdown) {
      // TODO: prepare a ProducerRecord with a String as key composed of firstName and lastName
      // TODO: as well as a message which is a ConnectionEvent
      val key = ???
      val record = ???

      // TODO: send the record to Kafka

      // slow down the loop to not monopolize your CPU
      Thread.sleep(1000)
    }

    producer.close()
  }

  // Message consumption
  def consumerLoop = {
    // Instantiating the consumer
    // toDeserializer comes from PlayJsonSupport and implements Serdes automatically from Play Json directives
    val consumer = new KafkaConsumer[String, ConnectionEvent](props, toDeserializer[String], toDeserializer[ConnectionEvent])
    // TODO: subscribe to the topic to receive the messages - topicName contains the name of the topic.

    // Consuming messages on our topic
    while (!consumerScheduler.isShutdown) {
      // TODO: fetch messages from the kafka cluster
      val records: ConsumerRecords[String, ConnectionEvent] = ???
      // TODO: print the received messages
    }

    consumer.close()
  }

  def buildProperties: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
    // Disable caching to print the aggregation value after each record
    // properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0")
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    properties
  }
}