from __future__ import print_function
import sys,json
from pyspark import SparkContext

if __name__ == "__main__":
  if len(sys.argv)<2:
    print("Please Enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName="Geo Tweets")

  tweets = sc.textFile(sys.argv[1])
  #getting tweet count
  tweet_cnt = tweets.count()
  print (tweet_cnt)
  #filtering tweets based on geo tags
  tweets_geo = tweets.filter(lambda tweet: json.loads(tweet)["geo"] is not None)
  #counting tweets containing geo tags
  geo_cnt = tweets_geo.count()
  proportion = (float(geo_cnt)/float(tweet_cnt))*100
  print (geo_cnt)
  #getting coordinates from geo tags
  coordinates = tweets_geo.map(lambda tweet: json.loads(tweet)["geo"]["coordinates"])
  latitudes = coordinates.map(lambda coordinate: coordinate[0])
  longitudes = coordinates.map(lambda coordinate: coordinate[1])
  centroid_lat = latitudes.mean()
  centroid_long = longitudes.mean()
  centroid = (centroid_lat,centroid_long)
  print (centroid)
  print (proportion)

  #6080302                                                                         
  #1868302                                                                         
( #38.990338432752786, -82.452943520471138)                                       
  #30.7271250671

