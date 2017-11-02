package com.supergloo.ml.streaming

import java.sql.Timestamp

import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

object UberStream {

  val localLogger = Logger.getLogger("UberDataStream")

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("Uber Stream")

    sparkConf.setIfMissing("spark.master", "local[5]")
    //    sparkConf.setIfMissing("spark.checkpoint.dir", checkpointDir)

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val kafkaTopicRaw = "raw_uber"
    val kafkaBroker = "127.0.01:9092"

    val topics: Set[String] = kafkaTopicRaw.split(",").map(_.trim).toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBroker)

    localLogger.info(s"connecting to brokers: $kafkaBroker")
    localLogger.info(s"kafkaParams: $kafkaParams")
    localLogger.info(s"topics: $topics")

    def parseUber(str: String): Uber = {
      val p = str.split(",")
      Uber(Timestamp.valueOf(p(0)), p(1).toDouble, p(2).toDouble, p(3))
    }

    val model = KMeansModel.load("/tmp/savemodel")

    val rawUberStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    rawUberStream.print // for demo purposes only

    val valuesDStream: DStream[String] = rawUberStream.map(_._2)

    valuesDStream.foreachRDD { rdd =>

      // There exists at least one element in RDD
      if (!rdd.isEmpty) {
        val count = rdd.count

        println("count received " + count)
        // Get the singleton instance of SparkSession
        val spark = SparkSession
          .builder()
          .config(rdd.sparkContext.getConf)
          .getOrCreate()

        import spark.implicits._

        val df = rdd.map(parseUber).toDF()
        // Display the top 20 rows of DataFrame
        println("uber data")
        df.show()

        // get features to pass to model
        val featureCols = Array("lat", "lon")
        val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
        val df2 = assembler.transform(df)

        // get cluster categories from  model
        val categories = model.transform(df2)
        categories.show

//        categories.createOrReplaceTempView("uber")

        // select values to join with cluster centers
        // convert results to JSON string to send to topic
//
//        val clust = categories.select($"dt", $"lat", $"lon", $"base", $"prediction".alias("cid")).orderBy($"dt")
//        clust.show(10)
      }
    }

    //Kick off
    ssc.start()

    ssc.awaitTermination()

    ssc.stop()
  }
}
