package com.justinrmiller.sparkstreaming

import com.justinrmiller.sparkstreaming.detectors.{OutlierDetector, WeatherEventDetector}
import com.redislabs.provider.redis._
import com.redislabs.provider.redis.streaming.ConsumerConfig
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {
  val logger: Logger = LogManager.getLogger(this.getClass.getName)

  logger.setLevel(Level.INFO)

  def main(args: Array[String]) {
    if (args.length != 3) {
      logger.error("Usage: java -jar <jarname> <redis-host> <redis-port> <redis-auth>")
    }

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.redis.host", args(0))
      .config("spark.redis.port", args(1))
      .config("spark.redis.auth", args(2))
      .getOrCreate()

    import spark.implicits._

    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val stream = ssc.createRedisXStream(Seq(ConsumerConfig("weather", "weather-events", "weather-consumer-001")))

    val detectors: Seq[WeatherEventDetector] = Seq(new OutlierDetector)

    // stream all the things
    stream.foreachRDD { rdd =>
      // consume events
      val events: RDD[WeatherEvent] = rdd.map { item => WeatherEvent(item.fields) }

      // generate state
      val states: RDD[((Long, Long, String), Iterable[WeatherEventState])] = events.flatMap { we =>
        detectors.flatMap { _.extractState(we) }
      }.groupBy(x => (x.customerId.get, x.sensorId.get, x.key.get))

      val aggregatedStates = states.map { state =>
        val key = state._1
        val groupedState = state._2

        val summedState = groupedState.foldLeft[WeatherEventState](WeatherEventStateMonoid.zero) { (a, b) =>
          WeatherEventStateMonoid.plus(a, b)
        }

        (key, summedState)
      }

      val detections = aggregatedStates.flatMap { state =>
        if (state._1._3 == detectors.head.key) {
          detectors.head.emit(state._2).map(_.message)
        } else {
          None
        }
      }

      logger.info("States:")

      states.foreach(logger.info(_))

      logger.info("Aggregated States:")

      aggregatedStates.foreach(logger.info(_))

      logger.info("Detections:")

      detections.foreach(logger.info(_))

      logger.info(events.toDF.printSchema())
      logger.info(events.toDF.show(100, false))
    }

    stream.start()
    ssc.start()
    ssc.awaitTermination()
  }
}
