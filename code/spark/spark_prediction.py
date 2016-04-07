import sys
from random import random
from operator import add

from pyspark import SparkContext
import json

from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from numpy import array

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.split(' ')]
    return LabeledPoint(values[0], values[1:])

# Load and parse the data
def parsePoint_lin(line):
    values = [float(x) for x in line.replace(',', ' ').split(' ')]
    return LabeledPoint(values[0], values[1:])

# Load and parse the data
def parsePoint_twitter(dict):
    popularity = float(dict['tweet']['orig_retweet_count'])+0.001
    video_length = float(dict['tweet']['orig_video_length_ms'])+0.001
    favorite_count = float(dict['tweet']['orig_favorite_count'])+0.001
    features = [favorite_count]
    LP =  LabeledPoint(popularity, features)
    #print LP
    return LP

# Load and parse the data
def parsePoint_twitter_log(dict):
    pop = float(dict['tweet']['orig_retweet_count'])+0.001
    if pop >= 500:
        popularity = 1
    else:
        popularity = 0
    video_length = float(dict['tweet']['orig_video_length_ms'])+0.001
    favorite_count = float(dict['tweet']['orig_favorite_count'])+0.001
    features = [video_length, favorite_count]
    LP =  LabeledPoint(popularity, features)
    #print LP
    return LP


if __name__ == "__main__":
    """
        Spark Analysis
    """

    #partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2

    sc = SparkContext(appName="SparkAnalysis")

    # load Twitter data
    #sm_twitter = sc.textFile("file:///root/mongoData/small_twitter.json")
    #print 'COUNT SMALL TWITTER: %d' % (sm_twitter.count())
    #print sm_twitter.first()
    twitter = sc.textFile("file:///root/mongoData/twitter.json")
    #print 'COUNT LARGE TWITTER: %d' % (twitter.count())
    twitter_data = twitter.map(lambda x: json.loads(x))
    print 'TWITTER DATA COUNT: %d' % (twitter_data.count())
    #print twitter_data.first()
    #print twitter_data.first()['created_at']

    #parsedData_twitter = twitter_data.map(parsePoint_twitter)

    # Build linear regression model
    #model_twitter = LinearRegressionWithSGD.train(parsedData_twitter)

    # Evaluate the model on training data
    #valuesAndPreds_twitter = parsedData_twitter.map(lambda p: (p.label, model_twitter.predict(p.features)))
    #print valuesAndPreds_twitter
    #print valuesAndPreds_twitter.count()
    #MSE_twitter = valuesAndPreds_twitter.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds_twitter.count()
    #print("Mean Squared Error = " + str(MSE_twitter))

    parsedData_twitter_log = twitter_data.map(parsePoint_twitter_log)

    # Build logistic regression model
    model_twitter_log = LogisticRegressionWithSGD.train(parsedData_twitter_log)

    # Evaluating the model on training data
    labelsAndPreds_twitter_log = parsedData_twitter_log.map(lambda p: (p.label, model_twitter_log.predict(p.features)))
    trainErr_twitter_log = labelsAndPreds_twitter_log.filter(lambda (v, p): v != p).count() / float(parsedData_twitter_log.count())
    print("Training Error = " + str(trainErr_twitter_log))


    # load YouTube data
    #sm_youtube = sc.textFile("file:///root/mongoData/small_youtube.json")
    #youtube = sc.textFile("file:///root/mongoData/youtube.json")
    #youtube_data = sm_youtube.map(lambda x: json.loads(x))
    #print youtube_data.count()
    #print youtube_data.first()

    # load Facebook data
    #sm_facebook = sc.textFile("file:///root/mongoData/small_facebook.json")
    #facebook = sc.textFile("file:///root/mongoData/facebook.json")
    #facebook_data = sm_facebook.map(lambda x: json.loads(x))
    #print facebook_data.count()
    #print facebook_data.first()

    #data = sc.textFile("sample_svm_data.txt")
    #parsedData = data.map(parsePoint)

    # Build the model
    #model = LogisticRegressionWithSGD.train(parsedData)

    # Evaluating the model on training data
    #labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
    #trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())
    #print("Training Error = " + str(trainErr))

    # Linear Regression model
    #data_lin = sc.textFile("lpsa.data")
    #parsedData_lin = data_lin.map(parsePoint_lin)

    # Build the model
    #model_lin = LinearRegressionWithSGD.train(parsedData_lin)

    # Evaluate the model on training data
    #valuesAndPreds_lin = parsedData_lin.map(lambda p: (p.label, model_lin.predict(p.features)))
    #MSE_lin = valuesAndPreds_lin.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds_lin.count()
    #print("Mean Squared Error = " + str(MSE_lin))

    sc.stop()


