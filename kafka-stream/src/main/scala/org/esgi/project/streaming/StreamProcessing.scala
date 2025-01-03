package org.esgi.project.streaming

import io.github.azhur.kafka.serde.PlayJsonSupport
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.kstream.{JoinWindows, TimeWindows, Windowed}
import org.esgi.project.streaming.models.{MeanLatencyForURL, Metric, Visit, VisitWithLatency}

import java.time.Duration
import java.util.Properties

object StreamProcessing extends PlayJsonSupport {

  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.serialization.Serdes._

  // TODO: Predeclared store names to be used, fill your first & last name
  val yourFirstName: String = ???
  val yourLastName: String = ???

  val applicationName = s"web-events-stream-app-$yourFirstName-$yourLastName"

  val visitsTopicName: String = "visits"
  val metricsTopicName: String = "metrics"

  val thirtySecondsStoreName: String = "VisitsOfLast30Seconds"
  val lastMinuteStoreName = "VisitsOfLastMinute"
  val lastFiveMinutesStoreName = "VisitsOfLast5Minutes"

  val thirtySecondsByCategoryStoreName: String = "VisitsOfLast30SecondsByCategory"
  val lastMinuteByCategoryStoreName = "VisitsOfLastMinuteByCategory"
  val lastFiveMinutesByCategoryStoreName = "VisitsOfLast5MinutesByCategory"
  val meanLatencyForURLStoreName = "MeanLatencyForURL"

  val props = buildProperties

  // defining processing graph
  val builder: StreamsBuilder = new StreamsBuilder

  // TODO: declared topic sources to be used
  val visits: KStream[String, Visit] = ???
  val metrics: KStream[String, Metric] = ???

  /**
   * -------------------
   * Part.1 of exercise
   * -------------------
   */
  // TODO: repartition visits per URL
  val visitsGroupedByUrl: KGroupedStream[String, Visit] = ???

  // TODO: implement a computation of the visits count per URL for the last 30 seconds,
  // TODO: the last minute and the last 5 minutes
  val visitsOfLast30Seconds: KTable[Windowed[String], Long] = ???

  val visitsOfLast1Minute: KTable[Windowed[String], Long] = ???

  val visitsOfLast5Minute: KTable[Windowed[String], Long] = ???

  /**
   * -------------------
   * Part.2 of exercise
   * -------------------
   */
  // TODO: repartition visits topic per category instead (based on the 2nd part of the URLs)
  val visitsGroupedByCategory: KGroupedStream[String, Visit] = ???

  // TODO: implement a computation of the visits count per category for the last 30 seconds,

  val visitsOfLast30SecondsByCategory: KTable[Windowed[String], Long] = ???

  // TODO: the last minute
  val visitsOfLast1MinuteByCategory: KTable[Windowed[String], Long] = ???

  // TODO: and the last 5 minutes
  val visitsOfLast5MinuteByCategory: KTable[Windowed[String], Long] = ???

  // TODO: implement a join between the visits topic and the metrics topic,
  // TODO: knowing the key for correlated events is currently the same UUID (and the same id field).
  // TODO: the join should be done knowing the correlated events are emitted within a 5 seconds latency.
  // TODO: the outputted message should be a VisitWithLatency object.
  val visitsWithMetrics: KStream[String, VisitWithLatency] = ???

  // TODO: based on the previous join, compute the mean latency per URL
  val meanLatencyPerUrl: KTable[String, MeanLatencyForURL] = ???

  // -------------------------------------------------------------
  // TODO: now that you're here, materialize all of those KTables
  // TODO: to stores to be able to query them in Webserver.scala
  // -------------------------------------------------------------

  def run(): KafkaStreams = {
    val streams: KafkaStreams = new KafkaStreams(builder.build(), props)
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    Runtime.getRuntime.addShutdownHook(new Thread(new Runnable() {
      override def run(): Unit = {
        streams.close()
      }
    }))
    streams
  }

  // autoloader from properties file in project
  def buildProperties: Properties = {
    val properties = new Properties()
    // Utilisez le port 19092 pour les brokers Kafka sous Conduktor, sinon 9092
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092")
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName)
    properties.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, "0")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, "-1")
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    properties
  }
}
