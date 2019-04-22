package org.apache.spark.examples

import scala.io.Source

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util.concurrent.{Executors, LinkedBlockingQueue}
import java.util.concurrent.ThreadLocalRandom
import java.util.Arrays
import java.util.Iterator
import java.util.List

class StreamingWordCount {

  case class SentenceEvent(timeStamp: Long, sentence: String)
  case class WordRecord(timeStamp: Long, word: String, count: Int)

  class Generator(sourcePath : String, batchSize: Int) {

    var tpool = Executors.newFixedThreadPool(1)
    var source = Source.fromFile(sourcePath)
    // lazy iterator returning batches, -1 to represent no timestamp
    var lines = source.getLines.grouped(batchSize) 

    def start(sc : SparkContext, 
              numRecords: Int,
              groupSize: Int,
              numReducers: Int,
              targetThroughput: Int) {
      val generatorThread = new Runnable {
        override def run() {
          // Initialize state RDD (guessing this chaining is necessary for FT)
          var stateRdd: RDD[(long, String, long)] = sc.parallelize(0 until numReducers, numReducers).mapPartitions(_ => Iterator())
          // Divide batchSize by groupSize?
          val numRecordsSeen = 0
          //val microBatchTimeSlice = batchSize / targetThroughput
          val recordTimestamp = System.currentTimeMillis
          (0 until numIterations by groupSize).map { i =>
            // Generate a group (Array[RDD])
            val thisGroup = (0 until groupSize).map { g => 
              recordTimeStamp += microBatchTimeSlice
              stateRdd = generateRDD(sc, recordTimeStamp, numReducers, targetThroughput)
              // TODO possibly checkpoint RDD
              stateRdd
            }.toArray
            recordTimestamp += microBatchTimeSlice 
            // Enqueue for processing
            log.info("Added group " + i)
            rddQueue.put(thisGroup)
            numRecordsSeen += (batchSize * groupSize) // TODO fix
            if (numRecordsSeen >= numRecords) {
              return
            }
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
                    recordTimeStamp: long, 
                    numReducers: Int,
                    targetThroughput: Int) {
      // Generate batches
      val dataRdd = sc.parallelize(0 until numPartitions, numPartitions).mapPartitions { _ =>
        val linesBatch = lines.next().zipWithIndex.map { case (line, index) => 
         if (index == 0) Event(recordTimeStamp, line) else Event(-1, line) 
        }
        linesBatch
      }
      // Wait until time slice complete
      val dataReadyRdd = dataRdd.mapPartitions { iter =>
        if (iter.hasNext) {
          val curTime = System.currentTimeMillis
          // Start processing after we are at T + B
          // as that is the time we generate data until
          val targetTime = recordTimeStamp + microBatchTimeSliceInMs
          if (curTime < targetTime) {
            log.debug("Sleeping for " + remaining + " ms")
            Thread.sleep(targetTime - curTime)
          }
          iter
        } else {
          iter
        }
      }
      // Process words (TODO fix, reduceByKey without ts)
      val counts = dataReadyRdd.flatMap { event => 
        val words = event.sentence.split(" ")
        val wordsWithTSandCounts = words.map.zipWithIndex { case (word, index) => 
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

  def processStream(sc : SparkContext, 
                    batchRDDs : LinkedBlockingQueue[Array[RDD[(long, String, long)]]], 
                    numItersInBatch : Int, 
                    groupSize : Int): Unit = {
      (0 until numItersInBatch by groupSize).map { x =>
        // Blocking for group x
        val batch = batchRDDs.take()
        // Picked up group x
        val funcs = Seq.fill(batch.length)(pairCollectFunc) // TODO define func
        long current = System.currentTimeMillis
        val results = sc.runJobs(batch, funcs)
        println("Round takes " + (System.currentTimeMillis() - current))
        results.foreach { result =>
          println(result.flatten.mkString("Latency: [", ":", "]"))
        }
      }
    }
  }

  def main(args : Array[String]) {

    // TODO add correct params
    if (args.length < 5) {
      println("Usage: StreamingWordCount [words_file] [checkpoint_path] [batch_size] [group_size] [num_reducers] [target_throughput]")
      System.exit(1)
    }

    val wordsFilePath = args[0]
    val checkpointPath = args[1]
    val batchSize = args[2]
    val groupSize = args[3]
    val numReducers = args[4]
    val targetThroughput = args[5]
    // num mappers?

    val sparkConf = SparkConf().setAppName("StreamingWordCount")
    val sc = SparkContext(sparkConf)
    sc.setCheckpointDir(checkpointPath)

    // Let all the executors join
    Thread.sleep(1000)

    // Warmup JVM
    for (i <- 1 to 20) {
      val rdd = sc.parallelize(0 until sc.getExecutorMemoryStatus.size, sc.getExecutorMemoryStatus.size)
      val b = rdd.map(x => (x % 2, 1L)).reduceByKey(_ + _)
      b.count
    }

    // Generate and process stream
    val generator = Generator(wordsFilePath, batchSize)
    generator.start(sc, numRecords, groupSize, numReducers, targetThroughput)
    processStream(sc)
    generator.stop()
    sc.stop()
  }
}
