package com.justinrmiller.sparkstreaming

import com.redislabs.provider.redis._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {

  def main(args: Array[String]) {
    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.host", "spark.redis.host")
      .config("spark.redis.port", "6379")
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

    val stream = ssc.createRedisXStream(Seq(ConsumerConfig("my-stream", "my-consumer-group", "my-consumer-1")))
    stream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
