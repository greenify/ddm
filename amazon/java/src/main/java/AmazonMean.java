import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.DoubleFunction;
import java.util.Arrays;
import java.util.List;

public class AmazonMean 
{
    public static void main(String[] args)
    {
        System.out.println(args[0]);
        JavaSparkContext sc = new JavaSparkContext();
        JavaRDD<String> file = sc.textFile(args[0]);
        JavaRDD<List<String>> lines = file.map(s -> Arrays.asList(s.split("\016")));

        JavaDoubleRDD ratings = lines.mapToDouble(new DoubleFunction<List<String>>()
        {
            public double call(List<String> s)
            {
                return Double.parseDouble(s.get(6));
            }
        });

        Double totalMean  = ratings.mean();
        System.out.printf("mean %.2f\n", totalMean); 
    }
}
