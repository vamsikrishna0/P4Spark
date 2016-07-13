from __future__ import print_function
import sys, json
from pyspark import SparkContext

import plotly;
plotly.tools.set_credentials_file(username='vamsikrishna.angajala', api_key='sk1n9hooza')

import plotly.plotly as py
import plotly.graph_objs as go

sc = SparkContext(appName="plotting")

#Plotting the heat map
topFreqPerYear = sc.textFile("hdfs://hadoop2-0-0/user/angajava/top25WordFrequencies/part-00000").map(lambda x:x.split())

totalPerYear = topFreqPerYear.map(lambda x: (int(x[1]), int(x[2]))).reduceByKey(lambda a,b : a+b).collectAsMap()
dataMap=topFreqPerYear.map(lambda x: (str(x[0]), int(x[1]), int(int(x[2])*1000/totalPerYear[int(x[1])])))

xaxis = dataMap.map(lambda x: x[1]).distinct().collect()
yaxis = dataMap.map(lambda x: x[0]).collect()

zaxis = dataMap.map(lambda x: (x[0], x[2])).groupByKey().mapValues(list).map(lambda x: x[1]).collect()

data = [
    go.Heatmap(
        z= zaxis,
           x= xaxis,
        y= yaxis
    )
  ]
plot_url = py.plot(data, filename='heatmapFor1gram2')
