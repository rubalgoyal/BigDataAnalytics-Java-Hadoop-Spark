import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.*;
import java.util.regex.Pattern;


public class CommonCouples {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: CommonCouples <input folder> <output folder>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("CommonCouples");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> fileLines = javaSparkContext.textFile(args[0]);
        // Get all the pairs of words and their counts
        JavaPairRDD<Tuple2<String, String>, Integer> commonCouples = fileLines
                .flatMap(line -> {
                        String[] words = line.split("[ ,.;:'\"&!?\\-_\n\t12345678910\\[\\]{}<>\\\\`~|=^()@#$%^*/+-]+"); 
                        List<Tuple2<String, String>> couples = new ArrayList<>();
                        for (int i = 0; i < words.length - 1; i++) {
                            Tuple2<String, String> pair = new Tuple2<>(words[i].toLowerCase(), words[i + 1].toLowerCase());
                            couples.add(pair);
                        }
                        return couples.iterator();
                })
                .mapToPair(
                        // This will sort the keys in an order and becomes easier to combine
                        tuple -> {
                            int compareValue = tuple._1.compareTo(tuple._2);
                            if (compareValue >= 0)
                                return  new Tuple2<>(new Tuple2<>(tuple._1, tuple._2), 1);
                            else
                                return  new Tuple2<>(new Tuple2<>(tuple._2, tuple._1), 1);
                        }
                );

        JavaPairRDD<Tuple2<String, String>, Integer> commonCoupleCount = commonCouples.reduceByKey(Integer::sum);
        // Interchange key and values so that we can sort by key on count and take top 10
        List<Tuple2<Integer, Tuple2<String, String>>> commonCoupleSorted = commonCoupleCount.mapToPair(
                            tuple -> new Tuple2<>( tuple._2, new Tuple2<>(tuple._1._1, tuple._1._2) )
                    )
                .sortByKey(false)
                        .take(10);

        JavaRDD<Tuple2<Integer, Tuple2<String, String>>> top10ValuesRDD = javaSparkContext.parallelize(commonCoupleSorted);
        top10ValuesRDD.map(tuple -> tuple._2 +" , " + tuple._1).saveAsTextFile(args[1]);
        javaSparkContext.stop();
    }

}
