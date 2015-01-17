import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    if(args.length < 2){
      println("No input file provided")
      System.exit(1)
    }
    val file = sc.textFile(args(0))
    val counts = file.flatMap(line => line.split(" "))
                     .map(word => (word, 1))
                     .reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))

    }

  }
