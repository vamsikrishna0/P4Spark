# Print the top25 words using Spark
# To run, do: spark-submit --master yarn-client oneGram.py hdfs://hadoop2-0-0/data/1gram

from __future__ import print_function
import sys, json
from pyspark import SparkContext


if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName="Top25FrequentWords")
  lines = sc.textFile(sys.argv[1],)
  freq = lines.map(lambda s: (s.split()[0], int(s.split()[2]))).reduceByKey(lambda a,b : a+b)
  
  #find top 25 words 
  freqWords = sc.parallelize(freq.takeOrdered(25, key = lambda (k,v): -v)).keys().collect()
  
  #Find top 25's individual frequencies
  freqPerYear = lines.map(lambda s: (s.split()[0], int(s.split()[1]), int(s.split()[2]))).filter(lambda (a,b,c): a in freqWords and b > 1800)
  topFreqPerYear = freqPerYear.map(lambda s : (str(s[0])+" "+str((s[1]-1800)/10), s[2])).reduceByKey(lambda a,b : a+b )
  results = topFreqPerYear.map(lambda (k,v): k+" "+ str(v)).coalesce(1)
  
  results.saveAsTextFile("top25WordFrequencies")
  
  
  sc.stop()
