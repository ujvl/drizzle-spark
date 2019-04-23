/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import scala.io.Source
import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.{Duration, Time}

import java.util.HashMap
import java.util.concurrent.{Executors, LinkedBlockingQueue}
import java.util.concurrent.ThreadLocalRandom

object StreamingWordCount {

  case class SentenceEvent(timeStamp: Long, sentence: String)
  case class WordRecord(timeStamp: Long, word: String, count: Int)
  type OutputRDD = RDD[(String, Long)]

  class Generator(sourcePath: String,
                  batchSize: Int,
                  queue: LinkedBlockingQueue[Array[OutputRDD]]) {

   @transient lazy val log = Logger.getLogger(getClass.getName)

    val SentenceLength = 100
    val r = scala.util.Random

    var source = Source.fromFile(sourcePath)
    var wordsSource = source.getLines.flatMap(line => line.split(" ")).toArray
    var rddQueue = queue
    var tpool = Executors.newFixedThreadPool(1)

    def start(sc: SparkContext,
              groupSize: Int,
              numMicroBatches: Int,
              numPartitions: Int,
              numReducers: Int,
              targetThroughput: Int) {
      val generatorThread = new Runnable {
        override def run() {
          var stateRdd: OutputRDD = sc.parallelize(0 until numReducers, numReducers)
                                      .mapPartitions(_ => Iterator())
          // Divide batchSize by groupSize?
          val microBatchTimeSlice = batchSize / targetThroughput
          var recordTimeStamp = System.currentTimeMillis
          (0 until numMicroBatches by groupSize).map { i =>
            val thisGroup = (0 until groupSize).map { g =>
              val iteration = i + g
              recordTimeStamp += microBatchTimeSlice
              // Generate a drizzle group
              stateRdd = generateRDD(stateRdd, recordTimeStamp, numPartitions,
                                     numReducers, targetThroughput)
              if (false) {
                // TODO define checkpoint condition
                stateRdd.checkpoint()
              }
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

    def generateRDD(stateRdd: OutputRDD,
                    recordTimeStamp: Long,
                    numPartitions: Int,
                    numReducers: Int,
                    targetThroughput: Int) : OutputRDD = {
      val sc = stateRdd.context

      val batchSizeLocal = batchSize
      val wordsSourceLocal = wordsSource
      val wordsSourceSizeLocal = wordsSourceLocal.size
      val sentenceLengthLocal = SentenceLength
      val rLocal = r

      // ----------------
      // Generate batches
      // ----------------
      val dataRdd = sc.parallelize(0 until numPartitions, numPartitions).mapPartitions { _ =>
        val linesBatch = (0 until batchSizeLocal).map { i =>
          val idx = rLocal.nextInt(wordsSourceSizeLocal - sentenceLengthLocal)
          val line = wordsSourceLocal.slice(idx, idx + sentenceLengthLocal).mkString(" ")
          if (i == 0) SentenceEvent(recordTimeStamp, line) else SentenceEvent(-1, line)
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

      // ---------------------------
      // Flat-map sentences to words
      // ---------------------------
      // TODO see how zipWithIndex().map() compares to iterating over list manually
      val countsRdd = dataReadyRdd.flatMap { event =>
        val words = event.sentence.split(" ")
        val wordsWithTSandCounts = words.zipWithIndex.map { case (word: String, index) =>
          // Only pass the timestamp to a single word of the sentence
          if (index == 0) WordRecord(event.timeStamp, word, 1) else WordRecord(-1, word, 1)
        }
        wordsWithTSandCounts
      }

      // Repartition for reduce step (zipPartitions) -- coalesce assumes numReducers < numMappers
      val repartitionedCountsRdd = countsRdd.coalesce(numReducers)

      // -----------------------------------------------------------
      // Reduce word counts of current batch along with state so far
      // -----------------------------------------------------------
      val newStateRdd = countsRdd.zipPartitions(stateRdd) { case (iterNew, iterOld) =>
        val countsMap = new HashMap[String, Long]
        iterOld.foreach { case (word, count) =>
          countsMap.put(word, count)
        }
        iterNew.foreach { wordRecord =>
          val word = wordRecord.word
          val count = wordRecord.count
          if (countsMap.containsKey(word)) {
            countsMap.put(word, count + countsMap.get(count))
          }
          else {
            countsMap.put(word, count)
          }
        }

        iterNew.foreach { wordRecord =>
          if (wordRecord.timeStamp != -1) {
            val lat = System.currentTimeMillis - wordRecord.timeStamp
            // TODO: write to file
          }
        }

        countsMap.asScala.iterator
      }
      newStateRdd.cache()

      return newStateRdd
    }
  }

  val pairCollectFunc = (iter: Iterator[(String, Long)]) => {
    iter.map(p => (p._1, p._2)).toArray
  }

  def processStream(sc: SparkContext,
                    batchRDDs: LinkedBlockingQueue[Array[OutputRDD]],
                    numMicroBatches: Int,
                    groupSize: Int): Unit = {
    (0 until numMicroBatches by groupSize).map { x =>
      // Blocking for group x
      val batch = batchRDDs.take()
      // Picked up group x
      val funcs = Seq.fill(batch.length)(pairCollectFunc)
      val current = System.currentTimeMillis
      val results = sc.runJobs(batch, funcs)
      println("Round takes " + (System.currentTimeMillis - current))
      results.foreach { result =>
        println(result.flatten.mkString("Latency: [", ":", "]"))
      }
    }
  }

  def main(args : Array[String]) {

    // TODO add correct params
    if (args.length < 8) {
      println("Usage: StreamingWordCount [words_file] [checkpoint_path]" +
                                       " [batch_size] [group_size] [num_microbatches]" +
                                       " [num_partitions] [num_reducers] [target_throughput]")
      System.exit(1)
    }

    val wordsFilePath = args(0)             // input path
    val checkpointPath = args(1)            // checkpoint path
    val batchSize = args(2).toInt           // size of micro-batches
    val groupSize = args(3).toInt           // size of drizzle scheduling group
    val numMicroBatches = args(4).toInt     // stopping condition
    val numPartitions = args(5).toInt       // number of mappers
    val numReducers = args(6).toInt         // number of reducers
    val targetThroughput = args(7).toInt    // target throughput

    val sparkConf = new SparkConf().setAppName("StreamingWordCount")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir(checkpointPath)

    // Let all the executors join
    Thread.sleep(2000)

    // Run a test job to make sure JARs are copied to executors etc.
    sc.parallelize(0 until sc.getExecutorMemoryStatus.size,
      sc.getExecutorMemoryStatus.size).foreach {
      x => Thread.sleep(10)
    }
    // Warmup JVM, make sure checkpoints work
    val rdd = sc.makeRDD(1 to numPartitions*100, numPartitions)
    rdd.cache()
    rdd.checkpoint()
    for (i <- 1 to 20) {
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
