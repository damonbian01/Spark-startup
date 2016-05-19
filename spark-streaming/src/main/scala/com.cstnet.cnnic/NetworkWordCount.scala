package com.cstnet.cnnic

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * nc -lk 9999
 */

object NetworkWordCount {
  def main (args: Array[String]){
    if (args.length < 2) {
      System.err.println("Usage: NetwordWordCount")
      System.exit(1)
    }

    val ip = args(0)
    val port = args(1).toInt

    val conf = new SparkConf().setAppName("NewwordWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines:DStream[String] = ssc.socketTextStream(ip, port)
    val words = lines.flatMap(_.split(" ")).flatMap(_.split("\t"))
    val wordCounts = words.map(x => (x,1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}