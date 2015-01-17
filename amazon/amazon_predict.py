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
training = inFile.filter(lambda x: x[1] % 10 < 4).map(
    lambda x: x[0]).cache()
validation = inFile.filter(lambda x: x[1] % 10 >= 4 & x[1] < 6).map(lambda x: x[0]).cache()
test = inFile.filter(lambda x: x[1] % 10 > 6).map(lambda x: x[0]).cache()

numTraining = training.count()
numValidation = validation.count()
numTest = test.count()

print "Training: %d, validation: %d, test: %d" % (numTraining, numValidation, numTest)

# train with different ranks
ranks = [6, 18]
lambdas = [0.1, 10.0]
numIters = [5, 20]
bestModel = None
bestValidationRmse = float("inf")
bestValidationAVG = float("inf")
bestRank = 0
bestLambda = -1.0
bestNumIter = -1

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

for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
    model = ALS.train(training, rank, numIter, lmbda)
    valAVG, valRmse = stats(model, validation, numValidation)
    print "RMSE (validation) = %f [AVG= %f] for the model trained with " % (valAVG, valRmse)+ \
          "rank = %d, lambda = %.1f, and numIter = %d." % (
              rank, lmbda, numIter)
    if (valRmse < bestValidationRmse):
        bestModel = model
        bestValidationRmse = valRmse
        bestValidationAVG = valAVG
        bestRank = rank
        bestLambda = lmbda
        bestNumIter = numIter

testRmse, testAVG = stats(bestModel, test, numTest)

# evaluate the best model on the test set
print "The best model was trained with rank = %d and lambda = %.1f, " % (bestRank, bestLambda) \
    + \
    "and numIter = %d, and its RMSE on the test set is %f. (AVG= %f)" % (
        bestNumIter, testRmse, testAVG)

# compare the best model with a naive baseline that always returns the
# mean rating
meanRating = training.union(validation).map(lambda x: x[2]).mean()
baselineRmse = math.sqrt(
    test.map(lambda x: (meanRating - x[2]) ** 2).reduce(add) / numTest)
improvement = (baselineRmse - testRmse) / baselineRmse * 100
print "The best model improves the baseline by %.2f" % (improvement) + "%."
