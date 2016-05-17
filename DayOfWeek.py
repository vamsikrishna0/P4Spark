from __future__ import print_function
import sys,json
import time
from pyspark import SparkContext

if __name__ == "__main__":
  if len(sys.argv)<2:
    print("Please Enter a filename")
    sys.exit(1)

  sc = SparkContext(appName="Average Tweet Length")
  
  tweets = sc.textFile(sys.argv[1])
  #Filtering tweets sent by PrezOno
  tweets_Ono = tweets.filter(lambda tweet: "PrezOno" in json.loads(tweet)["user"]["screen_name"].encode('ascii','ignore'))
  #Collecting created time stamp of PrezOne's tweets
  createdTime_Ono = tweets_Ono.map(lambda tweet: json.loads(tweet)["created_at"].encode('ascii','ignore'))
  #Extracting day of week and emitting a '1' for each day of week
  pairs = createdTime_Ono.map(lambda date: (time.strptime(date,'%a %b %d %H:%M:%S +0000 %Y').tm_wday,1))
  #Reducing the counts to find total count of each and every day of week
  counts = pairs.reduceByKey(lambda a,b: a+b)
  #Sorting the day of weeks based on count
  sorted_counts = counts.takeOrdered(3,key=lambda x: -x[1])
  print (sorted_counts)
 

