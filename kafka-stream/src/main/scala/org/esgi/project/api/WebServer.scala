package org.esgi.project.api

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import de.heikoseeberger.akkahttpplayjson.PlayJsonSupport
import org.apache.kafka.streams.{KafkaStreams, StoreQueryParameters}
import org.apache.kafka.streams.state.{QueryableStoreTypes, ReadOnlyKeyValueStore, ReadOnlyWindowStore}
import org.esgi.project.api.models.{MeanLatencyForURLResponse, VisitCountResponse}
import org.esgi.project.streaming.StreamProcessing
import org.esgi.project.streaming.models.{MeanLatencyForURL, Visit}

import java.time.Instant
import scala.jdk.CollectionConverters.IteratorHasAsScala

object WebServer extends PlayJsonSupport {
  def routes(streams: KafkaStreams): Route = {
    concat(
      path("visits" / Segment) { period: String =>
        get {
          period match {
            case "last-30-seconds" =>
              // TODO: get the last 30 seconds window store
//              val kvLast30Seconds: ReadOnlyWindowStore[String, Long] = ???
              // TODO: get the last 30 seconds window store as a list of VisitCountResponse containing the URL and the count
//              val keyValueSegementLast30Seconds: List[VisitCountResponse] = ???

              complete(
                List(VisitCountResponse("url", 1))
              )


            case "last-minute" =>
//              TODO: get the last minute window store
//              val kvLastFirstMinute: ReadOnlyWindowStore[String, Long] = ???
//              TODO: get the last minute window store as a list of VisitCountResponse containing the URL and the count
//              val keyValueSegmentLastMinute: List[VisitCountResponse] = ???

              complete(
                List(VisitCountResponse("url", 1))
              )

            case "last-5-minutes" =>
//              TODO: get the last 5 minutes window store
//              val kvLast5Minutes: ReadOnlyWindowStore[String, Long] = ???
//              TODO: get the last 5 minutes window store as a list of VisitCountResponse containing the URL and the count
//              val keyValueSegmentLast5Minutes: List[VisitCountResponse] = ???

              complete(
                List(VisitCountResponse("url", 1))
              )
          }
        }
      },
      path("visits" / "category" / Segment) { period: String =>
        get {
//          TODO: Same thing as above but for the category
          complete(
            List(VisitCountResponse("url", 1))
          )
        }
      },
      path("latency" / "beginning") {
        get {
//          TODO: get the mean latency for the URL store
//          val kvMeanLatency: ReadOnlyKeyValueStore[String, MeanLatencyForURL] = ???
//          TODO: get the mean latency for the URL as a list of MeanLatencyForURLResponse containing the URL and the mean latency
//          val keyValueMeanLatency: List[MeanLatencyForURLResponse] = ???
          complete(
            List(
              MeanLatencyForURLResponse("url", 1)
            )
          )
        }
      }
    )
  }
}
