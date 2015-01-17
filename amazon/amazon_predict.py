#!/usr/bin/python2

from __future__ import division
import math
from operator import add
import sys
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS

sc = SparkContext("local")

if len(sys.argv) < 2:
    print("no input file specified")
    sys.exit(1)

inputFile = sys.argv[1]
trainingFile = sys.argv[1]

print("input %s" % inputFile)

lines = sc.textFile(inputFile)
inFile = sc.textFile(inputFile).map(lambda x: x.split("\016")).map(lambda x: (int(x[3]), int(x[0]), float(x[6])))

inFile = inFile.zipWithUniqueId()
training = inFile.filter(lambda x: x[1] % 10 < 9).map(lambda x: x[0]).cache()
validation = inFile.filter(lambda x: x[1] % 10 == 9).map(lambda x: x[0]).cache()
numTraining = training.count()
numValidation = validation.count()

# train a recommendation model
model = ALS.train(training, rank=10, iterations=5)

# make predictions on (user, product) pairs from the test data
predictions = model.predictAll(validation.map(lambda x: (x[0], x[1])))
predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
      .join(validation.map(lambda x: ((x[0], x[1]), x[2]))) \
      .values()

avgErr = predictionsAndRatings.map(lambda x: abs(x[0] - x[1]) ).reduce(add) / float(numValidation)
stdErr = math.sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(numValidation))

print(predictionsAndRatings.take(5))
print("avgErr: %f" % avgErr)
print("stdErr: %f" % stdErr)
