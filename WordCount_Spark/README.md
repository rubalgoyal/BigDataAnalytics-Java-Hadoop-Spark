# Project 2: Better Inverted Index using Spark

* Author: Rubal Goyal
* Class: CS535 Section 1
* Semester: 2

## Overview

This Java program utilizes Apache Spark to build an inverted index from a set of text files. It parses the input files, divides them into words, and tracks the occurrence of each word in each file. The output is an organized data structure that connects words to lists of files and their word counts. The program then sorts the results based on word frequencies and if there is a tie then sorting happens on file names in alphabetical order.

## Reflection

Overall I found Spark is much easier to implement in Java compared to Hadoop from a programming perspective. The challenging part was reading all the files at once and then counting the words per file. This part consumed some time as it was not doing the partitioning of the output files and it was taking longer time compared to the Hadoop which was counter intuitive.

There were major two reasons for this issue:
1. I was using "local" in the Spark Context, which I believe was causing issues while running on the cluster.
   ```
   JavaSparkContext sc = new JavaSparkContext("local", "BetterInvertedIndexSpark");
   ```
   replaced above by
   ```
   SparkConf conf = new SparkConf().setAppName("BetterInvertedIndexSpark");
   ```
2. Second, by doing some research on the web, I found for faster turnaround, we can use the `minPartitions` parameter in `wholeTextFiles()` which makes it faster to run, so I fixed this issue by below line of code
   ```
   JavaPairRDD<String, String> inputFiles = sc.wholeTextFiles(args[0], 14);
   ```

## Compiling and Using
Assuming that the files to read from are available in ```../input/``` directory, please use below commands to execute the program (the output will be available in ```output/``` directory).
- For a Spark cluster that is up and running but using a shared local filesystem:
```
spark-submit --class BetterInvertedIndex --master spark://cscluster00.boisestate.edu:7077 spark-better-index.jar input output
```
- For a Spark cluster that is up and running along with a Hadoop cluster (please replace your username for hdfs system):
```
hdfs dfs -put input
spark-submit --class BetterInvertedIndex --master spark://cscluster00.boisestate.edu:7077 spark-better-index.jar hdfs://cscluster00:9000/user/username/input hdfs://cscluster00:9000/user/username/output
hdfs dfs -get output
```

## Results 

### Sample Output
The below screenshot shows the output for the _abate_ word. It first sorts the files by count and if there is a tie, it sorts the files in alphabetically order.
![Screenshot 2023-11-11 at 1 55 09 PM](https://github.com/cs535-fall23/p2-rubalgoyal/assets/105172154/543bdbfd-c69f-4e57-95d7-e8641d85c3db)


### Performance Comparison
The Spark program for larger datasets took only ~29 seconds however Hadoop version took around ~2 minutes. The Spark implementation is significantly faster.

#### Running time for Spark 

![TimeScreenshot](https://github.com/cs535-fall23/p2-rubalgoyal/assets/105172154/59d4b9d9-f074-4096-8476-5296191c4ae4)

#### Running time for MapReduce project

<img width="1440" alt="TimescrrenshotMapreduce" src="https://github.com/cs535-fall23/p2-rubalgoyal/assets/105172154/5a414581-5a2c-4296-bfd9-bf2f67f4e5d5">


## Sources used
- [How does Spark’s `wholeTextFiles()` work?](https://medium.com/@dfdeshom/how-does-sparks-wholetextfiles-work-be8e0bd45da0)
- Googling around some bugs and coding.
