package org.esgi.project.kafka

import java.util.concurrent.{Executors, ScheduledExecutorService}
import scala.language.implicitConversions

trait SimpleSchedulingLoops {
  val producerScheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()
  val consumerScheduler: ScheduledExecutorService = Executors.newSingleThreadScheduledExecutor()


  // implicit conversion to convert from function to Java Runnable
  implicit def runnable(f: => Unit): Runnable = new Runnable() {
    def run(): Unit = f
  }
}