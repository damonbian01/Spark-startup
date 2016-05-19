package com.cstnet.cnnic

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Tao Bian on 2016/5/18.
 */
object HdfsTest {

  def main (args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: HdfsTest <file>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("HdfsTest")
    val sc = new SparkContext(conf)
    val file: RDD[String] = sc.textFile(args(0))
    val mapped = file.map(str => str.length).cache()
    for (iter <- 1 to 10) {
      val start = System.currentTimeMillis()
      for (x <- mapped) x + 2
      val end = System.currentTimeMillis()
      println("Iteration" + iter + " took " + (end - start) + "ms")
    }
    sc.stop()
  }

}
