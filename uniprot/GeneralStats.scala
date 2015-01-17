package spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import spark.RDDCalc

import scala.collection.mutable.ListMap

object GeneralStats extends RDDCalc {

  def calc(sc: SparkContext, lines: RDD[String]) : String = {

    val counts = lines.flatMap(line => { val cells = line.split("Ä"); (cells(1).split("").zipWithIndex) })
    val stat= counts.countByKey
    val total = fo.foldLeft(0L)(_ + _._2)
    val relStat= stat.mapValues(v => v / 1.0 / total)
    val relStatSorted = new ListMap() ++ relStat.toList.sortBy(_._2)
    val relStatFormatted = fo3.mapValues(relStatSorted => "%1.2f".format(x))
    return relStatFormatted.mkString(";")

  }

}
