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

  // for count-min sketch
  val wCounters = args(4).toInt;
  val dRows = args(5).toInt;

  val countMinSketch = new CountMinSketch(wCounters, dRows)
  val globalTopCMS = countMinSketch.zero()

  //  create a StreamingContext, the main entry point for all streaming functionality.
  val ssc = new StreamingContext(sparkConf, Seconds(seconds));
  
  //  reduce logs verbosity
  val sc = ssc.sparkContext
  sc.setLogLevel("ERROR")

  var globalTopMap = Map[String, Int]()
  val merge = (ip1: String, ip2: String) => ip1 + "," + ip2

  def consume() {

    // create a DStream that represents streaming data from a directory source.
    val linesDStream = ssc.textFileStream(inputDirectory);

    // parse the stream. (line -> (IP1, IP2))
    val words = linesDStream.map(x => "(" + x.split("\t")(0) + "," + x.split("\t")(1) + ")")

    if (execType.contains("precise")) {
      val mapperIP = words.map(ip => (ip,1))
      val sumCountIP = mapperIP.reduceByKey(_ + _)

      sumCountIP.foreachRDD( rdd => {
        val batchSeq = rdd.collect().toSeq
        globalTopMap = (globalTopMap.toSeq ++ batchSeq).groupBy(_._1).mapValues(_.map(_._2).toList.sum)

        val globalTopK = globalTopMap.toSeq.sortBy(_._2).reverse.slice(0, TOPK).map({
          case (ip, count) => (count, ip)
        })
        
        val batchTopK = rdd.map({
          case (ip, count) => (count, ip)
        }).sortByKey(ascending=false).take(TOPK)

        println("This batch: %s".format(batchTopK.mkString("[", ",", "]")))
        println("Global batch: %s".format(globalTopK.mkString("[", ",", "]")))
        
      })

    } else if (execType.contains("approx")) {
      //TODO : Implement approx calculation (you will have to implement the CM-sketch as well
    }

    // Start the computation
    ssc.start()
  
    // Wait for the computation to terminate
    ssc.awaitTermination()
  }
}