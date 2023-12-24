# Project 3: Data Science with Spark

* Team Name: Team-06
* Author 1: Deepti Gururaj Baragi
* Author 2: Rubal Goyal
* Class: CS535 Section 1
* Semester: Fall 2023

## Overview

In this project, we utilize PySpark to explore the MovieLens dataset, emphasizing the discernment
of patterns and trends. Leveraging PySpark's capabilities, our analysis covers popular genres,
seasonal hits, yearly standouts, and the evolution of favorite movies over time. Delving deeper,
we examine how audience preferences vary based on gender and investigate rating discrepancies,
revealing instances where highly rated movies coexist with lower ratings.

## Reflection

Our experience with this project was notably smooth. Both of us had prior experience working with
the MovieLens dataset from CS533. We initiated this project by collaborating on Google Colab
Notebook before transitioning to the cluster without encountering major issues. The process
of searching for trends was particularly engaging, and we were happy to find that the analysis
flowed smoothly. Overall, this project offered practical insights into the scalability of our
analysis with PySpark, preparing us for real-world scenarios involving extensive datasets.

## Compiling and Using

The program can be compiled and run in Google Colab, prompting users to provide input paths and
customize parameters, including the number of rows to be displayed and preferences for trends
such as top movies based on the year or gender. Additionally, the program is compatible with a
Linux environment where Spark is installed. We successfully executed the program in both Google
Colab and cscluster environments.

Our Python file requires the following command line arguments
```
MovieLensProject.py <inputFilesFolderPath> <outputFolderPath> <intNumberForTopValues>
```

To run on the Spark cluster use the following command
```
spark-submit --master local[4] MovieLensProject.py <inputFilesFolderPath> <outputFolderPath> <intNumberForTopValues>
```

## Results 
1. `output-1million` this folder contains the output for 1 million dataset which we executed
on cluster. Since the size of the input wa too large so we did not add the input.

2.  `outputsplit` to enable input loading to git repo we split 1 million dataset so that the
size becomes less than 10 MB. The reduced size input is available in `ml-1m-split` and the folder
`outputsplit` is the output for same .

## Team work	

### Student 1 reflection
This project brought together our understanding of MovieLens, PySpark, SQL queries, and
large-scale data analysis. Collaborating with my team member was a great experience, as it
enabled me to gain knowledge through the division of tasks. Personally, I learned many things
while working on this project.

### Student 2 reflection
This project was quite interesting to work on. This was the first assignment I have done in
Python for this course. I have learned to use MySQL with PySpark. Initially, I faced some
difficulties, but after that, it went smoothly. I have tried to find different trends. As
this was a team project, it was good to collaborate and solve problems together and get a new
understanding from the thoughts of team members. Overall, tt was a fun experience, and I got
better at solving problems with data.



## Sources used

We haven't used any sources outside of the class materials.

----------
This README template is using Markdown. Here is a quick cheat sheet on Markdown tags:
[https://www.markdownguide.org/cheat-sheet/](https://www.markdownguide.org/cheat-sheet/).
To preview your README.md output, you can view it on GitHub or in eclipse.
