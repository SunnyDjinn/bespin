/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package io.bespin.scala.clustering

import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.rogach.scallop._

import scala.collection.mutable.{ListBuffer, Map, Set}

class ConfKMeans(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val k = opt[Int](descr = "number of output clusters", required = true)
  val iter = opt[Int](descr = "max number of iterations", required = false, default = Some(5))
  val stopwords = opt[String](descr = "path to a stopwords file", required = false, default = Some(""))
  verify()
}

object KMeans extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfKMeans(argv)

    val conf = new SparkConf().setAppName("K-Means").set("spark.default.parallelism", args.reducers().toString)
    val sc = new SparkContext(conf)
    val executorCoresNb = sc.getConf.getInt("spark.executor.cores", 4)
    val executorsNb = sc.getConf.getInt("spark.executor.instances", 2)
    val NB_WORKERS = executorCoresNb * executorsNb

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Number of output clusters: " + args.k())
    log.info("Max number of iterations: " + args.iter())
    log.info("Stopwords file: " + args.stopwords())
    log.info("Number of workers: " + NB_WORKERS)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val isStopwords = if (args.stopwords() != "") {true} else {false}

    // Euclidean Distance
    def distance(vector1: Map[String, Double], vector2: Map[String, Double]): Double = {
      val result = vector1.clone()
      for ((k, v) <- vector2) {
        result.update(k, result.getOrElse(k, 0d) - v)
      }
      math.sqrt(result.foldLeft(0d)((v1, v2) => math.pow(v1 + v2._2, 2)))
    }

    // Given two sequences of (Int, Map) tuples, returns whether or not they are different (stopping criterion)
    def areDifferent(tuples1: Seq[(Int, Map[String, Double])], tuples2: Seq[(Int, Map[String, Double])]): Boolean = {
      tuples1.sortBy(_._1)
      tuples2.sortBy(_._1)
      for ((center1, center2) <- tuples1.zip(tuples2)) {
        if (center1._2.toSeq.sorted != center2._2.toSeq.sorted) {
          return true
        }
      }
      false
    }

    val stopwords = Set[String]()
    if (isStopwords) {
      val words = sc.textFile(args.stopwords())
        .flatMap(line => {
          tokenize(line)
        })
        .collect()
        words.foreach(stopwords += _)
    }

    val stopwordsBC = sc.broadcast(stopwords)

    val corpus = sc.textFile(args.input())  // Each line is a document
      .mapPartitions(iter => if (isStopwords) { 
        val sequence = ListBuffer[(String, Map[String, Double])]()
        iter.foreach(line => {
          val tabSplitted = line.split("\t", 2)
          if (tabSplitted.length >= 2) {  // Rule out empty documents if any
            val wordVector = Map[String, Double]()
            tokenize(tabSplitted(1)).foreach(word => {
              if (! stopwordsBC.value.contains(word)) { // Do not include words that are in the stopwords list
                wordVector.update(word, wordVector.getOrElseUpdate(word, 0d) + 1d)
              }
            })
            sequence += ((tabSplitted.head, wordVector))
          }
        })
        sequence.toIterator
      } else {
      val sequence = ListBuffer[(String, Map[String, Double])]()
      iter.foreach(line => {
        val tabSplitted = line.split("\t", 2)
        if (tabSplitted.length >= 2) {
          val wordVector = Map[String, Double]()
          tokenize(tabSplitted(1)).foreach(word => {
            wordVector.update(word, wordVector.getOrElseUpdate(word, 0d) + 1d)
          })
          sequence += ((tabSplitted.head, wordVector))
        }
      })
      sequence.toIterator
    })
      .repartition(NB_WORKERS) // Smooth distribution of corpus in appropriate number of partitions
      .persist(StorageLevel.MEMORY_AND_DISK)


    // Randomly select K centroids
    var newCenters =
      corpus.takeSample(withReplacement = false, args.k()).zipWithIndex.map({
       case ((_, vector), index) => {
         (index, vector)
       }
      })
    var centersArr = Seq[(Int, Map[String, Double])]()

    var centersRDD = sc.parallelize(newCenters, numSlices = NB_WORKERS)  // Transform to RDD

    // Main loop. Iterate until stopping criterion met
    var i = 0
    do {
      i = i + 1

      centersArr = newCenters

      newCenters = corpus
          .cartesian(centersRDD)
          .mapPartitions(iter => {  // Calculate distances to each centroid
            val articleDistance = Map[String, (Map[String, Double], Int, Double)]()
            iter.foreach({
              case ((articleName, articleVector), (centerId, centerVector)) => {
                if (articleDistance.contains(articleName)) {
                  val dist = distance(centerVector, articleVector)
                  if (dist < articleDistance(articleName)._3) {
                    articleDistance.update(articleName, (articleVector, centerId, dist))
                  }
                } else {
                  articleDistance += ((articleName, (articleVector, centerId, distance(centerVector, articleVector))))
                }
              }
            })
            articleDistance.toIterator
          })
          .reduceByKey( {   // Keep the smallest distance/the closer centroid
            case((articleVector1, centerId1, distance1), (_, centerId2, distance2)) => {  // Same article vectors
              if (distance1 <= distance2) {
                (articleVector1, centerId1, distance1) // Keep the closest center
              }
              else {
                (articleVector1, centerId2, distance2)
              }
            }
          }, numPartitions = 4 * NB_WORKERS) 
          .mapPartitions(iter => {  // Add data points in a same cluster together and keep track of count
            val sequence = Map[Int, (Map[String, Double], Int)]()
            iter.foreach({
              case(_, (articleVector1, centerId, _)) => {
                if (sequence.contains(centerId)) {
                  val articleVectorCount2 = sequence(centerId)
                  sequence.remove(centerId)
                  sequence += ((centerId, (articleVector1 ++ articleVectorCount2._1.map({ case (k, v) => k -> (v +
                    articleVector1.getOrElse(k, 0d)) }), articleVectorCount2._2 + 1)))
                } else {
                  sequence += ((centerId, (articleVector1, 1)))
                }
              }
            })
            sequence.toIterator
          })
        .reduceByKey({ case ((articleVector1, count1), (articleVector2, count2)) => { // Average the mean centroid and keep track of count
          (articleVector1 ++ articleVector2.map({ case (k, v) => k -> (v + articleVector1.getOrElse(k, 0d)) }),
            count1 + count2)
         }
        }, numPartitions = 4*NB_WORKERS)
        .map({      // Compute the average
          case (centerId, (vector, count)) => {
            for ((key, value) <- vector) {
              vector.update(key, value / count)
            }
            (centerId, vector)
          }
        })
        .collect()  // If K and feature vector large, can run out of memory

      centersRDD = sc.parallelize(newCenters, numSlices = NB_WORKERS)

    } while (i < args.iter() && areDifferent(centersArr, newCenters))

    // Assign articles to their cluster
    val assignments = corpus
      .cartesian(centersRDD)
      .mapPartitions(iter => {
        val articleDistance = Map[String, (Int, Double)]()
        iter.foreach({
          case ((articleName, articleVector), (centerId, centerVector)) => {
            if (articleDistance.contains(articleName)) {
              val dist = distance(centerVector, articleVector)
              if (dist < articleDistance(articleName)._2) {
                articleDistance.update(articleName, (centerId, dist))
              }
            } else {
              articleDistance += ((articleName, (centerId, distance(centerVector, articleVector))))
            }
          }
        })
        articleDistance.toIterator
      })
      .reduceByKey( {
        case((centerId1, distance1), (centerId2, distance2)) => { 
          if (distance1 <= distance2) {
            (centerId1, distance1) 
          }
          else {
            (centerId2, distance2)
          }
        }
      })
      .map({
        case(articleName, (centerId, _)) => {
          (articleName, centerId)
          }
      })

    assignments.saveAsTextFile(args.output())
  }
}

