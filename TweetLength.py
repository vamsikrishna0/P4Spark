from __future__ import print_function
import sys,json
from pyspark import SparkContext

if __name__ == "__main__":
  if len(sys.argv)<2:
    print("Please Enter a filename")
    sys.exit(1)
  
  sc = SparkContext(appName="Average Tweet Length")
    
  #tweets = sc.textFile("hdfs://hadoop2-0-0/user/ghandisv/twitter_sample")
  tweets = sc.textFile(sys.argv[1])

  tweettexts_all = tweets.map(lambda tweet: json.loads(tweet)["text"].encode('ascii','ignore'))
  tweets_Ono = tweets.filter(lambda tweet: "PrezOno" in json.loads(tweet)["user"]["screen_name"].encode('ascii','ignore'))
  tweettexts_Ono = tweets_Ono.map(lambda tweet: json.loads(tweet)["text"].encode('ascii','ignore'))

  lengths_all = tweettexts_all.map(lambda text: len(text))
  lengths_Ono = tweettexts_Ono.map(lambda text: len(text))

  print (lengths_all.stats())
  print (lengths_Ono.stats())

