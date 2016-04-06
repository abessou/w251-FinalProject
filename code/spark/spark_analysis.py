import sys
from random import random
from operator import add

from pyspark import SparkContext
import json

if __name__ == "__main__":
    """
        Spark Analysis
    """

    #partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2

    sc = SparkContext(appName="SparkAnalysis")

    # load Twitter data
    sm_twitter = sc.textFile("file:///root/mongoData/small_twitter.json")
    print 'COUNT SMALL TWITTER: %d' % (sm_twitter.count())
    print sm_twitter.first()
    #twitter = sc.textFile("file:///root/mongoData/twitter.json")
    #print 'COUNT LARGE TWITTER: %d' % (twitter.count())
    twitter_data = sm_twitter.map(lambda x: json.loads(x))
    print 'TWITTER DATA COUNT: %d' % (twitter_data.count())
    print twitter_data.first()
    print twitter_data.first()['created_at']

    # load YouTube data
    sm_youtube = sc.textFile("file:///root/mongoData/small_youtube.json")
    #youtube = sc.textFile("file:///root/mongoData/youtube.json")
    youtube_data = sm_youtube.map(lambda x: json.loads(x))
    print youtube_data.count()
    print youtube_data.first()

    # load Facebook data
    sm_facebook = sc.textFile("file:///root/mongoData/small_facebook.json")
    #facebook = sc.textFile("file:///root/mongoData/facebook.json")
    facebook_data = sm_facebook.map(lambda x: json.loads(x))
    print facebook_data.count()
    print facebook_data.first()
 
    sc.stop()

