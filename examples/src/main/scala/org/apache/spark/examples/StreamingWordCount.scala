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

import java.io._
import java.util.HashMap
import java.util.concurrent.{Executors, LinkedBlockingQueue, ThreadLocalRandom}

import scala.collection.Iterator
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

import org.apache.log4j.Level
import org.apache.log4j.Logger

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, Time}

object StreamingWordCount {

  case class SentenceEvent(timeStamp: Long, sentence: String)
  type CountTSPair = (Int, Long)
  type WordRecord = (String, CountTSPair)
  type OutputRDD = RDD[(String, CountTSPair)]

  class Generator(sourcePath: String,
                  queue: LinkedBlockingQueue[Array[OutputRDD]]) {

    val SentenceLength = 100

    var source = Source.fromFile(sourcePath)
    var wordsSource = source.getLines.flatMap(line => line.split(" ")).toArray
    source.close()
    var rddQueue = queue

    var tpool = Executors.newFixedThreadPool(1)

    def start(sc: SparkContext,
              groupSize: Int,
              batchSize: Int,
              numMicroBatches: Int,
              numPartitions: Int,
              numReducers: Int,
              targetThroughput: Int) {
      val generatorThread = new Runnable {
        override def run() {
          // Broadcast the words source
          val wordsSourceBCVar = sc.broadcast(wordsSource)
          // Initialize state RDD
          var stateRdd: OutputRDD = sc.parallelize(0 until numReducers, numReducers)
                                      .mapPartitions(_ => Iterator())
          // Divide batchSize by groupSize?
          val microBatchTimeSlice = ((batchSize * 1.0 / targetThroughput) * 1000).toLong
          // Add an initial delay to the first timeStamp to account for initialization overheads
          // This is automatically accounted for in the Thread.sleep of the sentence generation
          var recordTimeStamp = System.currentTimeMillis + 10000
          (0 until numMicroBatches by groupSize).map { i =>
            val thisGroup = (0 until groupSize).map { g =>
              val iteration = i + g
              recordTimeStamp += microBatchTimeSlice
              // Generate a drizzle group
              stateRdd = generateRDD(stateRdd, wordsSourceBCVar, recordTimeStamp,
                                     batchSize, numPartitions, numReducers)
              if ((iteration % groupSize) == (groupSize - 1)) {
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
    }

    def generateRDD(stateRdd: OutputRDD,
                    wordsSourceBCVar: Broadcast[Array[String]],
                    recordTimeStamp: Long,
                    batchSize: Int,
                    numPartitions: Int,
                    numReducers: Int) : OutputRDD = {
      val sc = stateRdd.context

      // ----------------
      // Generate batches
      // ----------------
      val dataRdd = sc.parallelize(0 until numPartitions, numPartitions)
                      .mapPartitions { _ =>
        val linesBatch = (0 until batchSize).map { i =>
          val words = wordsSourceBCVar.value
          val sentenceLength = 100
          val idx = ThreadLocalRandom.current.nextInt(words.size - sentenceLength)
          val line = words.slice(idx, idx + sentenceLength).mkString(" ")
          if (i == 0) {
            SentenceEvent(recordTimeStamp, line)
          } else {
            SentenceEvent(-1, line)
          }
        }.iterator
        // Wait until time slice completes. Start processing after we are at T
        // as that is the time we generate data until
        val curTime = System.currentTimeMillis
        if (curTime < recordTimeStamp) {
          Thread.sleep(recordTimeStamp - curTime)
        }

        linesBatch
      }

      // ---------------------------
      // Flat-map sentences to words
      // ---------------------------
      // TODO see how zipWithIndex().map() compares to iterating
      // over list manually
      val countsRdd = dataRdd.flatMap { event =>
        val words = event.sentence.split(" ")
        val wordsWithTSandCounts = words.zipWithIndex.map {
          case (word: String, index) =>
          // Only pass the timestamp to a single word of the sentence
          // type: (word, (count, ts))
          if (index == 0) (word, (1, event.timeStamp)) else (word, (1, -1.toLong))
        }
        wordsWithTSandCounts
      }

      // -----------------------------------------------------------
      // Reduce word counts of current batch along with state so far
      // -----------------------------------------------------------
      val reducedRdd = countsRdd.reduceByKey(
        (a: CountTSPair, b: CountTSPair) => (a._1 + b._1, a._2.max(b._2)),
        numReducers
      )
      val newStateRdd = reducedRdd.zipPartitions(stateRdd) {
        case (iterNew, iterOld) =>
        val countsMap = new HashMap[String, CountTSPair]
        iterNew.foreach { case (word, countAndTs) =>
          countsMap.put(word, countAndTs)
        }
        iterOld.foreach { case (word, countAndTs) =>
          if (countsMap.containsKey(word)) {
            val curTup = countsMap.get(word)
            countsMap.put(word, (countAndTs._1 + curTup._1, curTup._2))
          } else {
            countsMap.put(word, countAndTs)
          }
        }
        countsMap.asScala.iterator
      }

      newStateRdd.cache()
      return newStateRdd
    }
  }

  val pairCollectFunc = (iter: Iterator[(String, CountTSPair)]) => {
    val now = System.currentTimeMillis
    iter.map { p =>
      val lat = if (p._2._2 != -1) now - p._2._2 else -1
      (p._1, p._2._1, p._2._2, lat)
      // word, count, ts, latency
    }.toArray
  }

  def processStream(sc: SparkContext,
                    batchRDDs: LinkedBlockingQueue[Array[OutputRDD]],
                    numMicroBatches: Int,
                    groupSize: Int): Unit = {
    val latencies = new ListBuffer[String]()
    var start_ts = -1 : Long
    (0 until numMicroBatches by groupSize).map { x =>
      // Blocking for group x
      val batch = batchRDDs.take()
      // Picked up group x
      val funcs = Seq.fill(batch.length)(pairCollectFunc)
      val current = System.currentTimeMillis

      if (groupSize == 1) {
        val results = sc.runJob(batch(1), pairCollectFunc)
        results.foreach { tups: Array[(String, Int, Long, Long)] =>
          tups.foreach { tup =>
            if (tup._3 != -1)
              println(tup._3.toString)
          }
        }
      } else {
        val cur = System.currentTimeMillis()
        val results = sc.runJobs(batch, funcs)
        println("Round takes " + (System.currentTimeMillis() - cur))
        //println("Group " + x.toString)
        results.foreach { in: Array[Array[(String, Int, Long, Long)]] =>
          in.foreach { tups =>
            tups.foreach { tup =>
              // 1 measurement per batch currently
              if (tup._4 != -1) {
                if (start_ts == -1) {
                  start_ts = tup._3
                }
                val time_since_start = tup._3 - start_ts
                val output_row = time_since_start + ", " + tup._4 + ", " + tup._1 + ", " + tup._2
                latencies += output_row
                //println(output_row)
              }
            }
          }
        }
      }
    }

    latencies.foreach { l =>
      println(l)
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
    val generator = new Generator(wordsFilePath, rddQueue)
    generator.start(sc, groupSize, batchSize, numMicroBatches,
                    numPartitions, numReducers, targetThroughput)
    processStream(sc, rddQueue, numMicroBatches, groupSize)
    generator.stop()
    sc.stop()
  }
}
