package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import spark.RDDCalc

object LongestRepeat extends RDDCalc {

  def calc(sc: SparkContext, lines: RDD[String]) : String = {

    val genes = lines.map(line => { val cells = line.split("Ä"); (cells(0).split('|')(1), cells(1)) })

    val res = genes.mapValues(v => countMax(v)).sortBy(_._2, false)

    res.take(6).mkString(";")
  }


  def countMax(w: String): Int = {
    var maxLen = 0
    var cLen = 0
    var cChar = w(0)
    for (x <- w) {
      if (x == cChar) {
        cLen += 1
      } else {
        cLen = 0; cChar = x;
      }
      if (cLen >= maxLen) {
        maxLen = cLen
      }
    }
    return maxLen
  }

}
