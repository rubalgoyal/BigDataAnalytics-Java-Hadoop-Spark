import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.Comparator;
import java.util.LinkedHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Builds an inverted index: each word followed by files it was found in.
 *
 *
 */
public class BetterInvertedIndex
{
    public static class BetterInvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        private final static Text word = new Text();
        private final static Text location = new Text();

        public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            location.set(fileName);

            String line = val.toString();
            StringTokenizer itr = new StringTokenizer(line.toLowerCase(),
                    " , .;:'\"&!?-_\n\t12345678910[]{}<>\\`~|=^()@#$%^*/+-");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, location);
            }
        }
    }


    public static class BetterInvertedIndexReducer extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> valueCountMap = new HashMap<>();

            for (Text value : values) {
                String valueStr = value.toString();
                valueCountMap.put(valueStr, valueCountMap.getOrDefault(valueStr, 0) + 1);
            }
            List<Map.Entry<String, Integer>> sortedValueCountList = new LinkedList<>(valueCountMap.entrySet());
            sortedValueCountList.sort((entry1, entry2) -> {
                int compareByValue = entry2.getValue().compareTo(entry1.getValue());
                if (compareByValue == 0) {
                    return entry1.getKey().compareTo(entry2.getKey());
                } else {
                    return compareByValue;
                }
            });
            Map<String, Integer> sortedValues = new LinkedHashMap<>();

            for (Map.Entry<String, Integer> entry : sortedValueCountList) {
                sortedValues.put(entry.getKey(), entry.getValue());
            }
            // Create a StringBuilder to build the output string
            StringBuilder output = new StringBuilder();
            boolean first = true;

            // Iterate through the map and build the output string
            for (Map.Entry<String, Integer> entry : sortedValues.entrySet()) {
                if (!first) {
                    output.append(", ");
                }
                first = false;
                output.append(entry.getValue()).append(" ").append(entry.getKey());
            }

            // Emit the key and the output string
            context.write(key, new Text(output.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.out.println("Usage: InvertedIndex <input path> <output path>");
            System.exit(1);
        }
        Job job = Job.getInstance(conf, "InvertedIndex");
        job.setNumReduceTasks(8);

        job.setJarByClass(BetterInvertedIndex.class);
        job.setMapperClass(BetterInvertedIndexMapper.class);
        job.setReducerClass(BetterInvertedIndexReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

