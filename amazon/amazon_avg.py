#!/usr/bin/python2

import sys
from pyspark import SparkContext

sc = SparkContext()

if len(sys.argv) < 2:
    print("no input file specified")
    sys.exit(1)

inputFile = sys.argv[1]

print("input %s" % inputFile)

lines = sc.textFile(inputFile)
rows = lines.map(lambda x: x.split("\016"))
mappi = rows.map(lambda x: (x[0], x))

ratings = mappi.combineByKey(lambda x: (float(x[6]),1), lambda x,y: (x[0] + float(y[6]), x[1] + 1), lambda x,y: (x[0] + y[0], x[1] + y[1] )).mapValues(lambda x: x[0] / x[1])

avgRating = ratings.mapValues(lambda x: "{:1.2f}".format(x))
totalMean = ratings.map(lambda x: x[1]).mean()
print(totalMean)
