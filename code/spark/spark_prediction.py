import sys
from random import random
from operator import add

from pyspark import SparkContext
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from numpy import array
import json

def load_data_from_file(sc, file_name):
    input = sc.textFile(file_name)
    data = input.map(lambda x: json.loads(x))
    print 'DATA COUNT: %d' % (data.count())
    #print data.first()
    return data

# Load and parse the data
def create_labeled_points_twitter(dict, reg_type):
    popularity = float(dict['tweet']['orig_retweet_count'])+0.001
    if reg_type == 'logistic':    
        if popularity >= 500.0:
            popularity = 1.0
        else:
            popularity = 0.0
    video_length_sec = float(dict['tweet']['orig_video_length_ms'])/1000.0 + 0.001
    favorite_count = float(dict['tweet']['orig_favorite_count'])+0.001
    features = [video_length_sec, favorite_count]
    LP =  LabeledPoint(popularity, features)
    #print LP
    return LP

def create_labeled_points_facebook(dict, reg_type):
    popularity = float(dict['total_likes'])+0.001
    if reg_type == 'logistic':    
        if popularity >= 500.0:
            popularity = 1.0
        else:
            popularity = 0.0
    video_length = float(dict['length'])+0.001
    total_comments = float(dict['total_comments'])+0.001
    features = [video_length, total_comments]
    LP =  LabeledPoint(popularity, features)
    #print LP
    return LP

def create_labeled_points_youtube(dict, reg_type):
    if dict['items'] != []:
        popularity = float(dict['items'][0]['statistics']['viewCount'])+0.001
    else:
        popularity = 1.0
    if reg_type == 'logistic':    
        if popularity >= 500.0:
            popularity = 1.0
        else:
            popularity = 0.0
    #if 'contentDetails' in dict['items'][0]:
    #    video_length = float(dict['items'][0]['contentDetails']['duration'])+0.001
    #else:
    #    video_length = 30.0
    video_length = 30.0
    if dict['items'] != []:
        favorite_count = float(dict['items'][0]['statistics']['favoriteCount'])+0.001
    else:
        favorite_count = 1.0
    features = [video_length, favorite_count]
    LP =  LabeledPoint(popularity, features)
    #print LP
    return LP

# Perform Spark prediction
def spark_prediction():
    """
        Spark Prediction
    """
    REGRESSION_TYPE = 'logistic'
    
    sc = SparkContext(appName="SparkPrediction")

    # load Twitter data
    #twitter_data = load_data_from_file(sc, "file:///root/mongoData/small_twitter.json")
    twitter_data = load_data_from_file(sc, "file:///root/mongoData/twitter.json")

    # load YouTube data
    #youtube_data = load_data_from_file(sc, "file:///root/mongoData/small_youtube.json")
    youtube_data = load_data_from_file(sc, "file:///root/mongoData/youtube.json")

    # load Facebook data
    #facebook_data = load_data_from_file(sc, "file:///root/mongoData/small_facebook.json")
    facebook_data = load_data_from_file(sc, "file:///root/mongoData/facebook.json")

    #create labeled points
    twitter_LP = twitter_data.map(lambda x: create_labeled_points_twitter(x, REGRESSION_TYPE))
    youtube_LP = youtube_data.map(lambda x: create_labeled_points_youtube(x, REGRESSION_TYPE))
    facebook_LP = facebook_data.map(lambda x: create_labeled_points_facebook(x, REGRESSION_TYPE))

    #combine all 3 datasets
    #rdd2 = rdd.union(rdd1)
    all_LP = twitter_LP.union(facebook_LP).union(youtube_LP)
    #all_LP = twitter_LP

    # Build logistic regression model
    model_log = LogisticRegressionWithSGD.train(all_LP)

    # Evaluating the model on training data
    labels_and_preds_log = all_LP.map(lambda p: (p.label, model_log.predict(p.features)))
    total = float(all_LP.count())
    trainErr_log = labels_and_preds_log.filter(lambda (v, p): v != p).count() / total
    print("Training Error = " + str(trainErr_log))
    print('ALL LP COUNT %d' % (all_LP.count()))

    # Build linear regression model
    #model_lin = LinearRegressionWithSGD.train(all_LP)

    # Evaluate the model on training data
    #labels_and_preds_lin = all_LP.map(lambda p: (p.label, model_lin.predict(p.features)))
    #print labels_and_preds_lin
    #print labels_and_preds_lin.count()
    #total = labels_and_preds_lin.count()
    #MSE = labels_and_preds_lin.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / total
    #print("Mean Squared Error = " + str(MSE))

    sc.stop()

if __name__ == "__main__":

    spark_prediction()

