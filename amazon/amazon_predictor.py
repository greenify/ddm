#!/usr/bin/python2

from __future__ import division
import math
from operator import add
import sys
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS
import itertools

sc = SparkContext()

if len(sys.argv) < 2:
    print("no input file specified")
    sys.exit(1)

inputFile = sys.argv[1]
trainingFile = sys.argv[1]

print("input %s" % inputFile)

lines = sc.textFile(inputFile)
inFile = sc.textFile(inputFile).map(lambda x: x.split("\016")).map(
    lambda x: (int(x[3]), int(x[0]), float(x[6])))

inFile = inFile.zipWithUniqueId()
training = inFile.filter(lambda x: x[1] % 10 < 6).map(
    lambda x: x[0]).cache()
validation = inFile.filter(lambda x: x[1] % 10 >= 7).map(lambda x: x[0]).cache()

numTraining = training.count()
numValidation = validation.count()

print "Training: %d, validation: %d" % (numTraining, numValidation)

def stats(model, data, num):

    # make predictions on (user, product) pairs from the test data
    predictions = model.predictAll(validation.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
        .join(validation.map(lambda x: ((x[0], x[1]), x[2]))) \
        .values()

    avgErr = predictionsAndRatings.map(
        lambda x: abs(x[0] - x[1])).reduce(add) / float(num)
    stdErr = math.sqrt(predictionsAndRatings.map(
        lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(num))

    return [avgErr, stdErr]

model = ALS.train(validation, 12, 10, 0.1)
testRmse, testAVG = stats(model, validation, numValidation)

# compare the best model with a naive baseline that always returns the
# mean rating
meanRating = validation.map(lambda x: x[2]).mean()
baselineRmse = math.sqrt(
    validation.map(lambda x: (meanRating - x[2]) ** 2).reduce(add) / numValidation)
improvement = (baselineRmse - testRmse) / baselineRmse * 100
print "The model improves the baseline by %.2f" % (improvement) + "%."

print "avgErr: %f" %  testAVG
print "stdErr: %f" %  testRmse
