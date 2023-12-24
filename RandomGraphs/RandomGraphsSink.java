import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.*;
import java.util.regex.Pattern;


public class RandomGraphsSink {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: RandomGraphs <numNodes> <probability> <outputFolder>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("RandomGraphs");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        int numNodes = Integer.parseInt(args[0]);
        double minProb = Double.parseDouble(args[1]);

        ArrayList<Integer> nodes = new ArrayList<>();
        for(int fromNode = 0; fromNode< numNodes; fromNode++){
            nodes.add(fromNode);
        }

        // Create an RDD of integers from 0 to numNodes - 1
        JavaRDD<Integer> nodesRDD = javaSparkContext.parallelize(
                nodes,
                javaSparkContext.defaultParallelism()
        );

        // Use flatMapToPair to generate edges in parallel
        JavaPairRDD<Integer, List<Integer>> graphRDD = nodesRDD.flatMapToPair(fromNode -> {
            HashMap<Integer, List<Integer>> graph = new HashMap<>();
            graph.put(fromNode, new ArrayList<>());
            for (int toNode = 0; toNode < numNodes; toNode++) {
                double prob = Math.random();
                if (prob >= minProb && fromNode != toNode)
                    graph.get(fromNode).add(toNode);
            }

            List<Tuple2<Integer, List<Integer>>> result = new ArrayList<>();
            if (!graph.isEmpty()) {
                result.add(new Tuple2<>(fromNode, graph.get(fromNode)));
            }

            return result.iterator();
        });

        // Combine the results from different partitions
        JavaPairRDD<Integer, List<Integer>> combinedGraphRDD = graphRDD.reduceByKey((list1, list2) -> {
            list1.addAll(list2);
            return list1;
        });

         //Now let's find the nodes from which no outgoing edge is there
        JavaPairRDD<Integer, List<Integer>> nodeNoOutgoingEdge = combinedGraphRDD.filter(pair -> pair._2().isEmpty());

        // Save the final graph to a text file
        nodeNoOutgoingEdge
                .map(tuple -> tuple._1 + "," + tuple._2)
                .saveAsTextFile(args[2]);

        // Stop the Spark context
        javaSparkContext.stop();

    }

}