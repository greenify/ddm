import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

object AmazonPred {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Spark Count"))

    if (args.length < 1) {
      println("No input file provided")
      System.exit(1)
    }

    val inputFile = args(0)
    val lines = sc.textFile(inputFile)
    val rows = lines.map(a => a.split("\016"))
    val inFile: RDD[Rating] = rows.map(x => (new Rating(x(3).toInt, x(0).toInt, x(6).toDouble)))

    val entireFile: RDD[Tuple2[Rating, Long]] = inFile.zipWithUniqueId()
    val training: RDD[Rating] = entireFile.filter((x: Tuple2[Rating, Long]) => (x._2 % 10) < 6).map(x => x._1).cache()
    val validation: RDD[Rating] = entireFile.filter((x: Tuple2[Rating, Long]) => (x._2 % 10) >= 6).map(x => x._1).cache()

    val numTraining = training.count()
    val numValidation = validation.count()

    println("Training: $numTraining, validation: $numValidation")

    val als = new ALS()
    als.setIterations(10)
    als.setLambda(0.1)
    als.setRank(12)
    val model: MatrixFactorizationModel = als.run(validation)
    val testValues: Tuple2[Double, Double] = stats(model, validation, numValidation)

    //compare the best model with a naive baseline that always returns the
    // mean rating
    val meanRating: Double = validation.map((x: Rating) => x.rating).mean()
    val valQ = validation.map[Double]((x: Rating) => (Math.pow(meanRating - x.rating, 2)))
    val baselineRmse = Math.sqrt(valQ.reduce((x: Double, y: Double) => x + y) / numValidation)
    val improvement = (baselineRmse - testValues._2) / baselineRmse * 100

    println(f"The model improves the baseline by ${improvement}%.2f %%.")
    println(f"avgErr: ${testValues._1}%.3f")
    println(f"stdErr: ${testValues._2}%.3f")

  }

  def stats(model: MatrixFactorizationModel, validation: RDD[Rating], num: Long): Tuple2[Double, Double] = {
    // make predictions on (user, product) pairs from the test data
    val predictions: RDD[Rating] = model.predict(validation.map((x: Rating) => (x.user, x.product)))
    val verifiedRatings: RDD[((Int, Int), Double)] = validation.map[((Int, Int), Double)]((x: Rating) => ((x.user, x.product), x.rating))
    val predictionsAndRatings: RDD[(Double, Double)] = predictions.map[Tuple2[Tuple2[Int, Int], Double]]((x: Rating) => ((x.user, x.product), x.rating))
      .join(verifiedRatings).values

    val avgErr = predictionsAndRatings.map[Double](x => Math.abs(x._1 - x._2)).reduce(_ + _) / 1.0 / num
    val stdErr = Math.sqrt(predictionsAndRatings.map(
      x => (Math.pow(x._1 - x._2, 2))).reduce(_ + _) / 1.0 / num);

    (avgErr, stdErr)
  }
}
