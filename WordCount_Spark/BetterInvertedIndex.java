import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.io.File;
import java.util.Comparator;
import org.apache.spark.api.java.function.Function2;
import java.util.List;
import java.util.LinkedList;

public class BetterInvertedIndex {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BetterInvertedIndexSpark");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<String, String> inputFiles = sc.wholeTextFiles(args[0], 14);

        JavaPairRDD<Tuple2<String, String>, Integer> wordCounts = inputFiles
                .flatMap(fileContent -> {
                    String fileName = new File(fileContent._1).getName();
                    String[] words = fileContent._2.split("[ ,.;:'\"&!?\\-_\n\t12345678910\\[\\]{}<>\\\\`~|=^()@#$%^*/+-]+");
                    return Arrays.stream(words)
                            .map(word -> new Tuple2<>(word.toLowerCase(), fileName))
                            .iterator();
                })
                .mapToPair(tuple -> new Tuple2<>(new Tuple2<>(tuple._1, tuple._2), 1) );

        JavaPairRDD<Tuple2<String, String>, Integer> wordFileCount = wordCounts.reduceByKey(Integer::sum);

        JavaPairRDD<String, Iterable<Tuple2<Integer, String>> > wordReduced = wordFileCount
                .mapToPair(
                                            tuple -> new Tuple2<>(tuple._1._1, new Tuple2<>(tuple._2, tuple._1._2))
                                    )
                .groupByKey()
                .mapValues(values -> {
                            List<Tuple2<Integer, String>> sortedValues = new LinkedList<>();
                            for (Tuple2<Integer, String> value : values) {
                                sortedValues.add(value);
                            }
                            sortedValues.sort(
                                    (tuple1, tuple2) ->{
                                        int compareByValue = tuple2._1.compareTo(tuple1._1);
                                        if(compareByValue == 0)
                                            return tuple1._2.compareTo(tuple2._2);
                                        else
                                            return compareByValue;
                                    }
                            );

                            return sortedValues;
                });



        JavaPairRDD<String, String> wordCombined = wordReduced
                .mapValues( values -> {
                    StringBuilder valuesCombined = new StringBuilder();
                    boolean first = true;
                    for(Tuple2<Integer,String> value : values){
                        if(!first)
                            valuesCombined.append(", ");
                        first = false;
                        valuesCombined.append(value._1 + " " + value._2);
                    }

                    return valuesCombined.toString();
                }).sortByKey();



        wordCombined.map(tuple -> tuple._1 + " " + tuple._2)
                .saveAsTextFile(args[1]);

        sc.stop();
    }
}

