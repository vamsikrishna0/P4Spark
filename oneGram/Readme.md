1Gram Data
=========

## Steps to run the program

1. First run the oneGram.py file using spark-submit.
2. The output is stored to a directory called "top25WordFrequencies".
3. For plotting the heat map, run the plot.py file in spark and see that it reads from "top25WordFrequencies" directory.
4. The output is a heatmap shown on a webpage.

# Commands #
==========

To run on cluster:
`spark-submit --master yarn-client oneGram.py <location of data on hdfs>` <br/>

`spark-submit --master yarn-client plot.py`

Note: Be sure the outputfile (top25Frequencies) does not exit in your HDFS home folder, before running the commands.

