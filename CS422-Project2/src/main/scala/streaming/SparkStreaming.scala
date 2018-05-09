package streaming;

import scala.io.Source
import scala.util.control._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming._
import scala.collection.Seq
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.Strategy
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;
import scala.collection.mutable.ListBuffer

class SparkStreaming(sparkConf: SparkConf, args: Array[String]) {

  val sparkconf = sparkConf;

  // get the directory in which the stream is filled.
  val inputDirectory = args(0)

  // number of seconds per window
  val seconds = args(1).toInt;

  // K: number of heavy hitters stored
  val TOPK = args(2).toInt;

  // precise Or approx
  val execType = args(3);

  //  create a StreamingContext, the main entry point for all streaming functionality.
  val ssc = new StreamingContext(sparkConf, Seconds(seconds));
  
  //  reduce logs verbosity
  val sc = ssc.sparkContext
  sc.setLogLevel("ERROR")

  def consume() {

    // create a DStream that represents streaming data from a directory source.
    val linesDStream = ssc.textFileStream(inputDirectory);

    // parse the stream. (line -> (IP1, IP2))
    val words = linesDStream.map(x => "(" + x.split("\t")(0) + "," + x.split("\t")(1) + ")")

    if (execType.contains("precise")) {
      var globalTopMap = Map[String, Int]()

      val preciseMapperIP = words.map(ip => (ip,1))
      val preciseSumCountIP = preciseMapperIP.reduceByKey(_ + _)

      preciseSumCountIP.foreachRDD( rdd => {
        if(rdd.count() != 0){
          val batchSeq = rdd.collect().toSeq
          val batchTopK = rdd.map({
            case (ip, count) => (count, ip)
          }).sortByKey(ascending=false).take(TOPK)

          globalTopMap = (globalTopMap.toSeq ++ batchSeq).groupBy(_._1).mapValues(_.map(_._2).toList.sum)
          val globalTopK = globalTopMap.toSeq.sortBy(_._2).reverse.slice(0, TOPK).map({
            case (ip, count) => (count, ip)
          })
          
          println("This batch: %s".format(batchTopK.mkString("[", ",", "]")))
          println("Global: %s".format(globalTopK.mkString("[", ",", "]")))
        }
      })

    } else if (execType.contains("approx")) {
      // for count-min sketch
      val wCounters = {
        if (args.size == 6)
          args(4).toInt
        else
          100
      }

      val dRows = {
        if (args.size == 6)
          args(5).toInt
        else
          10
      }

      val countMinSketch = new CountMinSketch(wCounters, dRows)
      var globalTopCMS = new CountMinSketch(wCounters, dRows)

      // hard-coded for testing purpose as the requirement of the project
      // note that the ip address should include bracket as well "()"
      val ipAddresses = List[String]("(161.69.48.219,161.69.45.5)")

      val approxMapperIP = words.map(ip => countMinSketch.map(ip))
      val approxSumCountIP = approxMapperIP.reduce(_ ++ _)

      approxSumCountIP.foreachRDD( rdd => {
        if (rdd.count() != 0){
          val batchSeq = rdd.first()
          val batchFrequency = ipAddresses.map( ip =>
            (ip, batchSeq.estimate(ip))).map({
              case (ip, count) => (count, ip)
            })

          globalTopCMS ++= batchSeq
          val globalFrequency = ipAddresses.map( ip =>
            (ip, globalTopCMS.estimate(ip))).map({
              case (ip, count) => (count, ip)
            })

          println("[CMS] This batch: %s".format(batchFrequency.mkString("[", ",", "]")))
          println("[CMS] Global: %s".format(globalFrequency.mkString("[", ",", "]")))
        }
      })
    
    }

    // Start the computation
    ssc.start()
  
    // Wait for the computation to terminate
    ssc.awaitTermination()
  }
}