package org.apache.spark.examples

import scala.io.Source
import scala.collection.Iterator
import scala.collection.JavaConverters._

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Duration, Time}

import java.util.concurrent.{Executors, LinkedBlockingQueue}
import java.util.concurrent.ThreadLocalRandom

class StreamingWordCount {

  case class SentenceEvent(timeStamp: Long, sentence: String)
  case class WordRecord(timeStamp: Long, word: String, count: Int)
  type OutputRDD = RDD[(String, Long)]

  class Generator(sourcePath: String,
                  batchSize: Int,
                  queue: LinkedBlockingQueue[Array[OutputRDD]]) {

    var tpool = Executors.newFixedThreadPool(1)
    var source = Source.fromFile(sourcePath)
    var rddQueue = queue
    // lazy iterator returning batches
    var lines = source.getLines.grouped(batchSize)

    def start(sc: SparkContext,
              groupSize: Int,
              numMicroBatches: Int,
              numPartitions: Int,
              numReducers: Int,
              targetThroughput: Int) {
      val generatorThread = new Runnable {
        override def run() {
          var stateRdd: OutputRDD = sc.parallelize(0 until numReducers, numReducers).mapPartitions(_ => Iterator())
          // Divide batchSize by groupSize?
          val microBatchTimeSlice = batchSize / targetThroughput
          var recordTimeStamp = System.currentTimeMillis
          (0 until numMicroBatches by groupSize).map { i =>
            // Generate a drizzle group
            recordTimeStamp += microBatchTimeSlice
            val thisGroup = (0 until groupSize).map { g =>
              stateRdd = generateRDD(sc, recordTimeStamp, numPartitions, numReducers, targetThroughput)
              // TODO possibly checkpoint RDD
              stateRdd
            }.toArray
            // Enqueue for processing
            rddQueue.put(thisGroup)
          }
        }
      }
      tpool.submit(generatorThread)
    }

    def stop() {
      tpool.shutdown()
      source.close()
    }

    // TODO see how zipWithIndex().map() compares to iterating over list manually
    def generateRDD(sc: SparkContext,
                    recordTimeStamp: Long,
                    numPartitions: Int,
                    numReducers: Int,
                    targetThroughput: Int) : OutputRDD = {
      // Generate batches
      val dataRdd = sc.parallelize(0 until numPartitions, numPartitions).mapPartitions { _ =>
        val linesBatch = lines.next().zipWithIndex.map { case (line, index) =>
         if (index == 0) SentenceEvent(recordTimeStamp, line) else SentenceEvent(-1, line)
        }.iterator
        linesBatch
      }
      // Wait until time slice completes
      val dataReadyRdd = dataRdd.mapPartitions { iter =>
        if (iter.hasNext) {
          // Start processing after we are at T as that is the time we generate data until
          val curTime = System.currentTimeMillis
          if (curTime < recordTimeStamp) {
            Thread.sleep(recordTimeStamp - curTime)
          }
          iter
        } else {
          iter
        }
      }
      // Process words (TODO fix, reduceByKey without ts)
      val counts = dataReadyRdd.flatMap { event =>
        val words = event.sentence.split(" ")
        val wordsWithTSandCounts = words.map.zipWithIndex { case (word: String, index) =>
          if (index == 0) WordRecord(event.timeStamp, word, 1) else WordRecord(1, word, 1)
        }
        wordsWithTSandCounts
      }.reduceByKey(_ + _, numReducers)
      // Output latencies
      val latencies = counts.foreach(count_tuple =>
        if (count_tuple._1 != -1) {
          val lat = System.currentTimeMillis - count_tuple._1
          // output lat to file/queue
        }
      )
      return latencies
    }
  }

  def processStream(sc: SparkContext,
                    batchRDDs: LinkedBlockingQueue[Array[OutputRDD]],
                    numMicroBatches: Int,
                    groupSize: Int): Unit = {
    (0 until numMicroBatches by groupSize).map { x =>
      // Blocking for group x
      val batch = batchRDDs.take()
      // Picked up group x
      val funcs = Seq.fill(batch.length)(pairCollectFunc) // TODO define func
      val current = System.currentTimeMillis
      val results = sc.runJobs(batch, funcs)
      println("Round takes " + (System.currentTimeMillis() - current))
      results.foreach { result =>
        println(result.flatten.mkString("Latency: [", ":", "]"))
      }
    }
  }

  def main(args : Array[String]) {

    // TODO add correct params
    if (args.length < 7) {
      println("Usage: StreamingWordCount [words_file] [checkpoint_path] [batch_size] [group_size] [num_microbatches] [num_partitions] [num_reducers] [target_throughput]")
      System.exit(1)
    }

    val wordsFilePath = args(0)
    val checkpointPath = args(1)
    val batchSize = args(2).toInt
    val groupSize = args(3).toInt
    val numMicroBatches = args(4).toInt // stopping condition
    val numPartitions = args(5).toInt
    val numReducers = args(6).toInt
    val targetThroughput = args(7).toInt

    val sparkConf = new SparkConf().setAppName("StreamingWordCount")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir(checkpointPath)

    // Let all the executors join
    Thread.sleep(2000)

    // Warmup JVM
    for (i <- 1 to 20) {
      val rdd = sc.parallelize(0 until sc.getExecutorMemoryStatus.size, sc.getExecutorMemoryStatus.size)
      val b = rdd.map(x => (x % 2, 1L)).reduceByKey(_ + _)
      b.count
    }

    // Generate and process stream
    val rddQueue = new LinkedBlockingQueue[Array[OutputRDD]]
    val generator = new Generator(wordsFilePath, batchSize, rddQueue)
    generator.start(sc, groupSize, numMicroBatches, numPartitions, numReducers, targetThroughput)
    processStream(sc, rddQueue, numMicroBatches, groupSize)
    generator.stop()
    sc.stop()
  }
}
