from __future__ import print_function
import sys,json
from pyspark import SparkContext

if __name__ == "__main__":
  if len(sys.argv)<2:
    print("Please Enter a filename")
    sys.exit(1)
  
  sc = SparkContext(appName="Average Tweet Length")
  tweets = sc.textFile(sys.argv[1])
  #Emitting all screen names with 1 to could total number of tweets from each user.
  users = tweets.map(lambda tweet: (json.loads(tweet)["user"]["screen_name"].encode('ascii','ignore'),1))
  #counting number of tweets from a particular screen name
  users_agg = users.reduceByKey(lambda a,b: a+b)
  #sorting and extracting top most screen name based on tweet counts
  sorted_counts = users_agg.takeOrdered(1,key=lambda x: -x[1])
  print (sorted_counts)
  
  #Emitting key-value pair of screen_name and tweet length for every tweet
  users_texts = tweets.map(lambda tweet: (json.loads(tweet)["user"]["screen_name"].encode('ascii','ignore'),len(json.loads(tweet)["text"])))
  #Finding average tweet length for every user
  users_texts_agg = users_texts.reduceByKey(lambda a,b: (a+b)/2)
  #Sorting users based on tweet average in descending order. Extracting only the top 10
  sorted_counts_desc = users_texts_agg.takeOrdered(10,key=lambda x: -x[1])
  print (sorted_counts_desc)
  #Sorting users based on tweet average in ascending order. Extracting only the top 10
  sorted_counts_asc = users_texts_agg.takeOrdered(10,key=lambda x: x[1])
  print (sorted_counts_asc)
 

