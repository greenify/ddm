#!/usr/bin/python2
# vim: set fileencoding=utf8 :

import sys
from pyspark import SparkContext
from collections import OrderedDict

sc = SparkContext("local")

if len(sys.argv) < 2:
    print("no input file specified")
    sys.exit(1)

inputFile = sys.argv[1]

print("input %s" % inputFile)

lines = sc.textFile(inputFile)


def countFlatter(line):
    cells = line.split(u"Ä")
    li = enumerate(list(cells[1]))
    return [(l[1], l[0]) for l in li]

counts = lines.flatMap(countFlatter)
stat = OrderedDict(sorted(counts.countByKey().iteritems()))
total = sum(stat.values())
relativeStat = {k: v / 1.0 / total for k, v in stat.iteritems()}

relativeStatForm = {k: "%.2f" % v for k, v in relativeStat.iteritems()}
print(relativeStatForm)
