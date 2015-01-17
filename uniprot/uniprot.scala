package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import spark.RDDCalc
import spark.LongestRepeat
import spark.GeneralStats

object UniprotMax {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count").setMaster("local"))

    if (args.length < 2) {
      println("<method> <inputfile>")
      System.exit(1)
    }

    val method = args(0)
    val lines = sc.textFile(args(1))
    //var m = (sc: SparkContext, lines: RDD[String]) => println("not found");
    var m: RDDCalc  = LongestRepeat

    method match {
      case "repeats" => m = LongestRepeat
      case "stats" => m = GeneralStats 
      case non => println(non + " is not a valid method.")
    }

    val res = m.calc(sc, lines)
    println(res)

  }

}
