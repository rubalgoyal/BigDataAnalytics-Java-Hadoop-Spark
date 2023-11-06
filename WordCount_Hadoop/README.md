# Project 1: Project 1: Better Inverted Index Program

* Author: Rubal Goyal
* Class: CS535 Section 1
* Semester: 2

## Overview

In this program, we are counting the word occurrence in the file. The __Mapper__ emits a word as the key and a file as the value for each occurrence of the world in the file. After intermediate sort and shuffle, the __Reducer__ counts the different name of files for each world and then sort the occurrences in reverse order, and if there is a tie then sorting happens on file name alphabetically order. Finally, the reducer concatenates the occurrence space filename in the comma separator string and emits.

## Reflection

While doing this project, I got a better understanding of how MapReduce runs on Hadoop. When I started the project I was facing issues while generating the output of the inverted index, but after talking to the Professor, I got a better understanding of how local files work on HDFS.

The mapper phase was easy to understand and write  but in the Reducer part, I was not sure how I would do the sorting. I read some articles on Google and read about how we can use Hash Maps and Linked List for sorting purposes. Initially, nothing works for me then I debug the code and make changes. After a few debugging, I started receiving the correct output. It was quite interesting to work on this Project, I learned so many things and feel more confident now to work on Hadoop and MapReduce programs.

## Compiling and Using 

### To run
```
hdfs dfs -put input 
hdfs dfs -ls
hadoop jar better-index.jar input output
hdfs dfs -get output
```
### Time Command
```
hdfs dfs -put etext-all etext-input
time hadoop jar better-index.jar etext-input output-new
hdfs dfs -get output-new
```
## Results 

### Final Output

<img width="1016" alt="Final_output" src="https://github.com/cs535-fall23/p1-rubalgoyal/assets/105172154/ed3d7d5c-420a-4ce8-a423-283932dd73c9">


### Time Output

<img width="1300" alt="Time_output" src="https://github.com/cs535-fall23/p1-rubalgoyal/assets/105172154/d9816d11-9254-4c7b-bf31-f7538e56923d">



## Sources used

https://www.geeksforgeeks.org/linkedhashmap-class-in-java/

