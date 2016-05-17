package com.cstnet.cnnic

import java.io.File

import org.apache.spark.{SparkContext, SparkConf}

object DFSReadWriteTest {

  private val NumParams = 2

  private var localFilePath: File = new File(".")
  private var dfsDirPath: String = ""

  private def printUsage(): Unit = {
    val usgae: String = "DFS Read/Write\n\n" +
      "Usage: localFilePath dfsDir\n" +
      "localFilePath - (string)\n" +
      "dfsDir - (string)"

    println(usgae)
  }

  private def parseArgs(args: Array[String]): Unit = {
    if (args.size != NumParams) {
      printUsage()
      System.exit(1)
    }

    localFilePath = new File(args(0))
    if (!localFilePath.exists()) {
      System.err.println("Give path (" + args(0) + ") does not exist.\n")
      printUsage()
      System.exit(1)
    }

    if (!localFilePath.isFile) {
      System.err.println("Give path (" + args(0) + ") is not a file.\n")
      printUsage()
      System.exit(1)
    }

    dfsDirPath = args(1)
  }

  private def readFile(filename: String): List[String] = {
    import scala.io.Source._
    val fileIter: Iterator[String] = fromFile(filename).getLines()
    val fileContents: List[String] = fileIter.toList
    fileContents
  }

  private def localWordCount(fileContents: List[String]): Int = {
    fileContents.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .groupBy(str => str)                //Map[str,List[string]]
      .mapValues(lst => lst.size)         //Map[str,size]
      .values
      .sum
  }

  def main (args: Array[String]) {
    parseArgs(args)

    println("Performing local word count.")
    val fileContents = readFile(localFilePath.toString)
    val fileLocalWordCount = localWordCount(fileContents)
    println("Local word count has been finished and the result is " + fileLocalWordCount)

    println("Creating SparkConf")
    val conf = new SparkConf().setAppName("Word Count")

    println("Creating SparkContext")
    val sc = new SparkContext(conf)

    println("Writing local file to DFS")
    val dfsFilename = dfsDirPath + "/dfs_read_write_test"
    val fileRDD = sc.parallelize(fileContents)
    fileRDD.saveAsTextFile(dfsFilename)

    println("Reading file from DFS and running Word Count")
    val readFileRDD = sc.textFile(dfsFilename)

    val dfsWordCount = readFileRDD
      .flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.size > 0)
      .map(str => (str,1))
      .countByKey()       //Map[String, Long], count the num of key
      .values
      .sum

    sc.stop()

    println("DFS word count has been finished and the result is " + dfsWordCount)
  }

}