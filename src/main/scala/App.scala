import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

case class HealthData (heartDisease: Double, bmi: Double, smoking: Double, alcohol: Double, stroke: Double,
 physicalHealth: Double, mentalHealth: Double, diffWalking: Double, sex: Double, age: Double, race: Double,
 diabetic: Double, activity: Double, generalHealth: Double, sleep: Double, asthma: Double, kidneyDisease: Double,
 skinCancer: Double)

object App {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("NameOfApp").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("/Users/krishnanshugupta/Cal Poly/369_CSC/testspark/src/heart_2020_cleaned.csv")

    val processData = data.flatMap { line =>
      val fields = line.split(",")
      fields match {
        case Array(hd, b, s, a, st, ph, mh, dw, sex, age, race, d, act, gh, sl, ast, kd, sc) =>
          Some(HealthData(
            if (hd == "Yes") 1.0 else 0.0,
            b.toDouble,
            if (s == "Yes") 1.0 else 0.0,
            if (a == "Yes") 1.0 else 0.0,
            if (st == "Yes") 1.0 else 0.0,
            ph.toDouble,
            mh.toDouble,
            if (dw == "Yes") 1.0 else 0.0,
            if (sex == "Male") 1.0 else 0.0,
            age match {
              case "18-24" => 1.0
              case "25-29" => 2.0
              case "30-34" => 3.0
              case "35-39" => 4.0
              case "40-44" => 5.0
              case "45-49" => 6.0
              case "50-54" => 7.0
              case "55-59" => 8.0
              case "60-64" => 9.0
              case "65-69" => 10.0
              case "70-74" => 11.0
              case "75-79" => 12.0
              case "80 or older" => 13.0
              case _ => 0.0
            },
            race match {
              case "White" => 1.0
              case "Black" => 2.0
              case "American Indian/Alaskan Native" => 3.0
              case "Asian" => 4.0
              case "Hispanic" => 5.0
              case "Other" => 6.0
              case _ => 0.0
            },
            d match {
              case "No, borderline diabetes" => 0.0
              case "No" => 0.0
              case "Yes" => 1.0
              case "Yes (during pregnancy)" => 1.0
            },
            if (act == "Yes") 1.0 else 0.0,
            gh match {
              case "Excellent" => 1.0
              case "Very good" => 2.0
              case "Good" => 3.0
              case "Fair" => 4.0
              case "Poor" => 5.0
              case _ => 0.0
            },
            sl.toDouble,
            if (ast == "Yes") 1.0 else 0.0,
            if (kd == "Yes") 1.0 else 0.0,
            if (sc == "Yes") 1.0 else 0.0
          ))
        case _ => None
      }
    }
    val clean_data = processData.map(h => h.productIterator.mkString(", "))
    clean_data.saveAsTextFile("/Users/krishnanshugupta/Cal Poly/369_CSC/testspark/src/output")

    sc.stop()
  }
}




/*
    val data = sc.textFile("/Users/krishnanshugupta/Cal Poly/369_CSC/testspark/src/heart_2022_withz_nans.csv")

    val rows = data.map(line => line.split(",").map(_.trim))

    // Filter out rows where any column is an empty value (null or blank)
    val cleanedRows = rows.filter(row => row.forall(_.nonEmpty))

    // Convert the cleaned rows back to a string RDD
    val cleanedData = cleanedRows.map(row => row.mkString(","))
    println(data.count())
    println(cleanedData.count())
    cleanedData.coalesce(1).saveAsTextFile("/Users/krishnanshugupta/Cal Poly/369_CSC/testspark/src/output")
*/