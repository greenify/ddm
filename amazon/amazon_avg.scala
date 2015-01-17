import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object AmazonAVG {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count")))

    if(args.length < 1){
      println("No input file provided")
      System.exit(1)
    }

    val lines = sc.textFile(args(0))

    // do some standard mapping
    val rows = lines.map(a => a.split("\016"))
    val mappi = rows.map(x => (x(0),x))

    //var p2 = mappi.combineByKey(x => 0.0, (x:Double,v:Array[String]) => x, (v:Double,v2:Double) => v + v2

      // reduce to (rating, ratingsPerItem) -> calc avg
      var ratings = mappi.combineByKey(x => (x(6).toDouble, 1), (z:(Double,Int),v) => (z._1 + v(6).toDouble, z._2 + 1), (v:(Double, Int),v2:(Double,Int)) => (v._1 + v2._1, v._2 + v2._2)).mapValues(x => x._1 / x._2)

      // resolve tuple to avg
      val avgRating = ratings.mapValues("%1.2f".format(_))
      val totalMean = ratings.map(_._2).mean()

      println(totalMean)

    }

  }
