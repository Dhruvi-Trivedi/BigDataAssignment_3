package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.Seq

object a3 {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: spark-submit --class streaming.a3 --master yarn --deploy-mode client a3.jar hdfs:///input hdfs:///output")
      System.exit(1)
    }

    val inputDir = args(0)
    val outputDir = args(1)

    val checkpointDir = "/s4146514/checkpoint"

    val conf = new SparkConf().setAppName("A3-Streaming")
    val ssc = new StreamingContext(conf, Seconds(3))

    // Enable checkpointing for updateStateByKey
    ssc.checkpoint(checkpointDir)

    val lines = ssc.textFileStream(inputDir)

    // Atomic counters to produce unique 3-digit suffixes
    val task1Seq = new java.util.concurrent.atomic.AtomicInteger(1)
    val task2Seq = new java.util.concurrent.atomic.AtomicInteger(1)
    val task3Seq = new java.util.concurrent.atomic.AtomicInteger(1)

    // preprocessing function
    def preprocess(line: String): Array[String] = {
      if (line == null) return Array.empty
      line.split(" ")
        .map(_.trim.toLowerCase)
        .filter(w => w.matches("^[A-Za-z]+$") && w.length >= 3)
    }

    // Task 1
    // For each RDD of DStream, count word frequency and save to HDFS
    val wordsDStream: DStream[String] = lines.flatMap(preprocess)

    wordsDStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val counts = rdd.map(word => (word, 1)).reduceByKey(_ + _)
        val out = counts.map { case (w, c) => s"$w\t$c" }
        val seq = task1Seq.getAndIncrement()
        val path = s"$outputDir/task1-%03d".format(seq)
        out.saveAsTextFile(path)
      }
    }

    // Task 2
    // For each RDD, generate ordered co-occurrence pairs (i != j) and count within the RDD (batch)
    val pairsPerLine: DStream[(String, Int)] = lines.flatMap { line =>
      val ws = preprocess(line)
      val n = ws.length
      if (n <= 1) Seq.empty
      else {
        for {
          i <- 0 until n
          j <- 0 until n
          if i != j
        } yield (s"${ws(i)}\t${ws(j)}", 1)
      }
    }

    pairsPerLine.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val counts = rdd.reduceByKey(_ + _)
        val out = counts.map { case (pair, c) => s"$pair\t$c" }
        val seq = task2Seq.getAndIncrement()
        val path = s"$outputDir/task2-%03d".format(seq)
        out.saveAsTextFile(path)
      }
    }

    // Task 3
    // Continuously accumulate co-occurrence frequency of word pairs using updateStateByKey
    // (UG requirement: output the accumulated co-occurrence frequencies every 3-second interval)

    // Prepare DStream[(pairString, 1)] same as pairsPerLine
    val pairCountsDStream: DStream[(String, Int)] = pairsPerLine

    // Update function for stateful accumulation
    val updateFunc = (newVals: Seq[Int], state: Option[Int]) => {
      val newSum = newVals.sum + state.getOrElse(0)
      Some(newSum)
    }

    val stateDStream = pairCountsDStream.updateStateByKey[Int](updateFunc)

    // At each interval, output the accumulated co-occurrence frequencies of every word pair
    stateDStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val out = rdd.map { case (pair, c) => s"$pair\t$c" }
        val seq = task3Seq.getAndIncrement()
        val path = s"$outputDir/task3-%03d".format(seq)
        out.saveAsTextFile(path)
      }
    }

    // Start streaming
    ssc.start()
    ssc.awaitTermination()
  }
}
