package com.training.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import java.io.File

import org.apache.hadoop.fs.{FileSystem, Path}

class ScalaFunctionPassing(@transient val sc:SparkContext,
                           val inputFileLocation:String,
                           val outputFileLocation:String,
                           val query:String) extends Serializable {
  @transient val conf = sc.hadoopConfiguration
  @transient val hdfs = FileSystem.get(conf)

  def countMatches():ScalaCount = {
    val rdd = sc.textFile(inputFileLocation)

    val matchesFunctionReference = getMatchesFunctionReference(rdd)

    val matchesFunctionReferenceCount = matchesFunctionReference.count()

    val matchesFieldReference = getMatchesFieldReference(rdd)
    val matchesFieldReferenceCount = matchesFieldReference.count()

    val matchesNoReference = getMatchesNoReference(rdd)
    val matchesNoReferenceCount = matchesNoReference.count()

    val totalcount = new ScalaCount(matchesFunctionReferenceCount,
                                    matchesFieldReferenceCount,
                                    matchesNoReferenceCount)

    saveLog(totalcount)

    totalcount
  }

  def saveLog(totalcount: ScalaCount): Unit = {
    val log = Array("Matches Function Reference Count: " + totalcount.getMatchesFunctionReference(),
                    "Matches Field Reference Count: " + totalcount.getMatchesFieldReference(),
                    "Matches No Reference Count: " + totalcount.getMatchesNoReference())

    val logFile = outputFileLocation + File.separator + "log"

    val logFilePath = new Path(logFile)

    var text = sc.parallelize(log)

    if (hdfs.exists(logFilePath)) hdfs.delete(logFilePath, true)

    text.coalesce(1).saveAsTextFile(logFile)
  }

  def isMatch(text:String): Boolean = {
    text.contains(query)
  }

  def getMatchesFunctionReference(rdd: RDD[String]): RDD[String] = {
    // Problem: "isMatch" means "this.isMatch", so we pass all of "this"
    rdd.filter(isMatch)
  }

  def getMatchesFieldReference(rdd: RDD[String]): RDD[Array[String]] = {
    // Problem: "query" means "this.query", so we pass of all "this"
    rdd.map(x => x.split(query))
  }

  def getMatchesNoReference(rdd:RDD[String]): RDD[Array[String]] = {
    // Safe: extract just the field we need into a local variable
    val query_ = this.query
    rdd.map(x => x.split(query_))
  }
}

object ScalaFunctionPassing {
  def main(args:Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = new SparkContext(sparkConf)
    val inputFileLocation = args(0)
    val outputFileLocation = args(1)
    val query = args(2)

    val func = new ScalaFunctionPassing(sc, inputFileLocation, outputFileLocation, query)
    func.countMatches()

  }
}
