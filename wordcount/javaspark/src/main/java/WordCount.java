import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;
import java.util.Arrays;

public class WordCount 
{
    public static void main(String[] args)
    {
        System.out.println(args[0]);
        JavaSparkContext sc = new JavaSparkContext();
        JavaRDD<String> file = sc.textFile(args[0]);
        JavaRDD<String> words = file.flatMap(new FlatMapFunction<String, String>()
        {
            public Iterable<String> call(String s)
            {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>()
        {
            public Tuple2<String, Integer> call(String s)
            {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>()
        {
            public Integer call(Integer a, Integer b)
            {
                return a + b;
            }
        });
        counts.saveAsTextFile(args[1]);
    }
}
