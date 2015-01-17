#!/usr/bin/python2
# vim: set fileencoding=utf8 :

import sys
from pyspark import SparkContext

sc = SparkContext()

if len(sys.argv) < 3:
    print("no input file specified")
    sys.exit(1)

inputFile = sys.argv[1]
outFile = sys.argv[2]

print("input %s" % inputFile)

file = sc.textFile(inputFile)
counts = file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile(outFile)
