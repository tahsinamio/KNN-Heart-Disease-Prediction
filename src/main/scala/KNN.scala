import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

import scala.io._
import scala.util.Random
import scala.math._

object KNN {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("KNN").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Load and parse the data
    val data = Source.fromFile("src/output/part-00000").getLines.toList
    val parsedData = data.map(s => Vectors.dense(s.split(", ").map(_.toDouble)))

    // Shuffle data
    val shuffledData = Random.shuffle(parsedData)

    // figure out index to split on
    val fraction = 0.7 // fraction of data to sample
    val split_idx = round(shuffledData.size * fraction).toInt

    // split test and train data
    val test = shuffledData.take(split_idx)
    val train = shuffledData.drop(split_idx)

    // make k and test read-only and viewable to all the different rdds across dif machines
    // broadcast is supposed to be used for small things, hopefully this isn't too big
    val k = 20 // temp value
    val k_val = sc.broadcast()
    val train_info = sc.broadcast(train)

    // distribute, at this point every computer has a chunk of data
    val test_rdd = sc.parallelize(test)

    // compute all the distances in knn:
    // for entry in test_rdd, need to compute distance between entry to all vals in train_info
    // maybe here return (entry, distance)
    // compute set of samples N with k smallest distances for entry (sort by distance, take(k))
    // return the majority label of samples in N
    // maybe here return (entry, N)
    // compare N and the actual classification and note if it's right or not
    // collect/aggregate/etc to finally analyze and get all the info (f1, accuracy, etc)

    sc.stop()
  }
}