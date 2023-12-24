Write a Spark program that generates a random directed graph for n vertices. Each possible edge in the graph is equally likely to be present using a specified probability. The inputs to the program are the number of of vertices $n$ and the $0 \leq probability \leq 1$. The output should be adjanceny lists of the following form (node id, (list of neighbors with edges from node id)):
```
(1, (2,4,5))
(2, (1,3))
(3, (1,5))
(4, ())
(5, (2,4))
```

Also, write a Spark program to identify all the sinks in the graph. A \textbf{sink} is a node that has no outgoing edges.
