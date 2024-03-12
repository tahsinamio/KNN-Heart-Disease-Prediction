import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
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
    val parsedData = data.map(_.split(", ").map(_.toDouble))

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
    val k = 21 // temp value
    val k_val = sc.broadcast(k)
    val train_info = sc.broadcast(train)

    // distribute, at this point every computer has a chunk of data
    val test_rdd = sc.parallelize(test)

    // compute all the distances in knn:
    // for entry in test_rdd, need to compute distance between entry to all vals in train_info
    val entryLblList = test_rdd.map(x => (x, train_info.value.map(y =>
        (y, sqrt(pow(x(1)-y(1), 2) + pow(x(2)-y(2), 2) + pow(x(3)-y(3), 2) + pow(x(4)-y(4), 2)
        + pow(x(5)-y(5), 2) + pow(x(6)-y(6), 2) + pow(x(7)-y(7), 2) + pow(x(8)-y(8), 2) + pow(x(9)-y(9), 2)
        + pow(x(10)-y(10), 2) + pow(x(11)-y(11), 2) + pow(x(12)-y(12), 2) + pow(x(13)-y(13), 2)
        + pow(x(14)-y(14), 2) + pow(x(15)-y(15), 2) + pow(x(16)-y(16), 2) + pow(x(17)-y(17), 2))))
        .sortBy({case (entry, distance) => distance}).take(k_val.value).map(_._1(0))))
    // entryLblList = (entry as list of doubles, list of labels)
    // take max count label
    val entryCountList = entryLblList.map({case (entry, labelList) => (entry, labelList.groupBy(identity).mapValues(_.size).maxBy(_._2))})
    // get the actual label of entry and the predicted label
    val rlLabelnewLabel = entryCountList.map({case (entry, countList) => (entry(0), countList._1)})
    // get results
    val results = rlLabelnewLabel.map({
      case (0.0, 0.0) => "TN"
      case (1.0, 1.0) => "TP"
      case (1.0, 0.0) => "FN"
      case (0.0, 1.0) => "FP"
    }).foreach(println)

    // map with totals
    // WARNING DOES NOT WORK YET (OR IT TAKES A LONG TIME)
    // get rid of the foreach(println) above before trying
    //val analyse = results.groupBy(identity).mapValues(_.size).foreach(println)
    sc.stop()

  }
}