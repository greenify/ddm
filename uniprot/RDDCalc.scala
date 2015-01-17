package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

trait RDDCalc {

  def calc(sc: SparkContext, lines: RDD[String]) : String

}
