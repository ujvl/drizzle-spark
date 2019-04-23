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
  case class WordRecord(timeStamp: Long, word: String, count: Int)
  type OutputRDD = RDD[(String, Long)]

  class Generator(sourcePath: String,
                  queue: LinkedBlockingQueue[Array[OutputRDD]]) {

    val SentenceLength = 100

    var source = Source.fromFile(sourcePath)
    var wordsSource = source.getLines.flatMap(line => line.split(" ")).toArray
    source.close()
    var rddQueue = queue

    val r = scala.util.Random
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
                                     batchSize, numPartitions, numReducers, targetThroughput)
              if (false) {
                // TODO define checkpoint condition
                stateRdd.checkpoint()
              }
              stateRdd
            }.toArray
            // Enqueue for processing
            println("Generated group " + i)
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
                    numReducers: Int,
                    targetThroughput: Int) : OutputRDD = {
      val sc = stateRdd.context
      // For some reason need to make copies of variables in local
      // scope for the variables to be usable by spark jobs.
      val sentenceLengthLocal = SentenceLength
      val rLocal = r

      // ----------------
      // Generate batches
      // ----------------
      val dataRdd = sc.parallelize(0 until numPartitions, numPartitions)
                      .mapPartitions { _ =>
        val linesBatch = (0 until batchSize).map { i =>
          val words = wordsSourceBCVar.value
          val idx = rLocal.nextInt(words.size - sentenceLengthLocal)
          val line = words.slice(idx, idx + sentenceLengthLocal).mkString(" ")
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
          if (index == 0) {
            WordRecord(event.timeStamp, word, 1)
          }
          else {
            WordRecord(-1, word, 1)
          }
        }
        wordsWithTSandCounts
      }

      // Repartition for reduce step (zipPartitions)
      // coalesce assumes numReducers < numMappers
      val repartitionedCountsRdd = countsRdd.coalesce(numReducers)

      // -----------------------------------------------------------
      // Reduce word counts of current batch along with state so far
      // -----------------------------------------------------------
      val newStateRdd = countsRdd.zipPartitions(stateRdd) {
        case (iterNew, iterOld) =>
        // hacky way to write out latencies
        val writer = new BufferedWriter(
          new FileWriter("/home/ubuntu/drizzle-spark/log.txt", true))

        var timestamps = new ArrayBuffer[Long]()
        val countsMap = new HashMap[String, Long]

        iterOld.foreach { case (word, count) =>
          countsMap.put(word, count)
        }
        iterNew.foreach { wordRecord =>
          val word = wordRecord.word
          val count = wordRecord.count
          if (countsMap.containsKey(word)) {
            countsMap.put(word, count + countsMap.get(word))
          }
          else {
            countsMap.put(word, count)
          }

          if (wordRecord.timeStamp != -1) {
            timestamps += wordRecord.timeStamp
          }

        }

        timestamps.foreach { ts =>
          val now = System.currentTimeMillis
          val lat = now - ts
          writer.append(lat.toString + "\n")
        }

        writer.close

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
