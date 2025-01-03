package org.esgi.project.kafka

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.streams.StreamsConfig
import org.esgi.project.kafka.models.ConnectionEvent

import java.time.{Duration, OffsetDateTime}
import java.util.{Properties, UUID}
import java.util.concurrent.{ScheduledFuture, TimeUnit}
import scala.jdk.CollectionConverters._

object MessageProcessing extends PlayJsonSupport with SimpleSchedulingLoops {


  // TODO: fill your first name & last name
  val yourFirstName: String = ???
  val yourLastName: String = ???

  val applicationName = s"simple-app-$yourFirstName-$yourLastName"
  val topicName: String = "connection-events"

  val propsProducer: Properties = buildProducerProperties
  val propsConsumer: Properties = buildConsumerProperties

  def run(): ScheduledFuture[_] = {
    producerScheduler.schedule(producerLoop, 1, TimeUnit.SECONDS)
    consumerScheduler.schedule(consumerLoop, 1, TimeUnit.SECONDS)
  }

  // TODO: implement message production
  def producerLoop(): Unit = {
    // Instantiating the producer
    // toSerializer comes from PlayJsonSupport and implements Serdes automatically from Play Json directives
    val producer = new KafkaProducer[String, ConnectionEvent](propsProducer, toSerializer[String], toSerializer[ConnectionEvent])

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
  def consumerLoop(): Unit = {
    // Instantiating the consumer
    // toDeserializer comes from PlayJsonSupport and implements Serdes automatically from Play Json directives
    val consumer = new KafkaConsumer[String, ConnectionEvent](propsConsumer, toDeserializer[String], toDeserializer[ConnectionEvent])
    // TODO: subscribe to the topic to receive the messages - topicName contains the name of the topic.

    // Consuming messages on our topic
    while (!consumerScheduler.isShutdown) {
      // TODO: fetch messages from the kafka cluster
      val records: ConsumerRecords[String, ConnectionEvent] = ???
      // TODO: print the received messages
    }

    consumer.close()
  }

  def buildProducerProperties: Properties = {
    val properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092")
    properties.put(ProducerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips")
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, applicationName)
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    properties
  }

  def buildConsumerProperties: Properties = {
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092")
    properties.put(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, "use_all_dns_ips")
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, applicationName)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, applicationName)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties
  }
}