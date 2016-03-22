import sys
from itertools import ifilter
from requests_oauthlib import OAuth1Session
from requests.exceptions import ChunkedEncodingError
import json
import time
import datetime

from DataSource import DataSource

def isVideo(tweet):
  if 'extended_entities' in tweet:
    if 'media' in tweet['extended_entities']:
      for i in range(len(tweet['extended_entities']['media'])):
        if tweet['extended_entities']['media'][i]['type'] == 'video':
          return True
      return False
    else:
      return False
  else:
    return False 

class TwitterDataIngestSource(DataSource):
  """Ingest data from Twitter"""

  def __init__(self, config, data_store):
    self.config = config
    self.data_store = data_store
    self.update_items = []

  def __iter__(self):
    if 'track' in self.config:
      self.track = self.config['track']
    else:
      self.track = 'ski,surf,board'

    auth = OAuth1Session(
      self.config['consumer_key'], 
      client_secret = self.config['consumer_secret'],
      resource_owner_key = self.config['access_token'],
      resource_owner_secret = self.config['access_token_secret']
    )

    request = auth.post(
      'https://stream.twitter.com/1.1/statuses/filter.json',
      data = 'track=' + self.track,
      stream = True
    )

    # filter out empty lines sent to keep the stream alive
    self.source_iterator = ifilter(lambda x: x, request.iter_lines())

    return self

  def next(self):
    # Returns the next NEW tweet
    while True:
      try:
        next_tweet = json.loads(self.source_iterator.next())
        if isVideo(next_tweet):
          filtered_tweet = self.processTweet(next_tweet)
          if filtered_tweet != {}:
            break
      except ChunkedEncodingError:
        print('Chunked Encoding Error next')
        self.__iter__()
        continue
    return filtered_tweet
  
  def getUpdateItems(self):
    if self.update_items == []:
      return None
    else:
      list_copy = list(self.update_items)
      self.update_items = []
      return list_copy
      
  def updateTweet(self, id_str, rt_history):
    # finish this method
    # make sure you handle the case where rt_history is [] for some reason
    update = ({'ID':id_str},
              {'$set':{'tweet.orig_retweet_count':rt_history['orig_retweet_count'],
              'tweet.orig_favorite_count':rt_history['orig_favorite_count'],
              'last_modified':datetime.datetime.utcnow().isoformat()},
              '$push':{'tweet.rt_history':rt_history}
              })
    self.data_store.update_one(update)

  def processTweet(self, tweet):
    f_tweet = {}
    if 'retweeted_status' in tweet:
      t_object = tweet['retweeted_status']
      rt_history = {}
      rt_history['rt_id_str'] = tweet['id_str']
      rt_history['rt_created_at'] = time.strftime('%Y-%m-%dT%H:%M:%S',
        time.strptime(str(tweet['created_at']),'%a %b %d %H:%M:%S +0000 %Y'))
      rt_history['rt_text'] = tweet['text']
      rt_history['orig_retweet_count'] = t_object['retweet_count']
      rt_history['orig_favorite_count'] = t_object['favorite_count']
      # If the tweet is in the database, then return {} and update the update
      # items list
      orig_id_str = t_object['id_str']
      updates = self.data_store.find({'ID':orig_id_str})
      if updates != []:
        self.updateTweet(orig_id_str, rt_history)
        return f_tweet
      else:
        f_tweet['retweet'] = 1
        f_tweet['rt_history'] = []
        f_tweet['rt_history'].append(rt_history)
    else:
      f_tweet['retweet'] = 0
      f_tweet['rt_history'] = []      
      t_object = tweet
      orig_id_str = t_object['id_str']
    
    f_tweet['orig_id_str'] = orig_id_str
    f_tweet['orig_created_at'] = time.strftime('%Y-%m-%dT%H:%M:%S',
      time.strptime(str(t_object['created_at']),'%a %b %d %H:%M:%S +0000 %Y'))
    f_tweet['orig_text'] = t_object['text']
    f_tweet['orig_retweet_count'] = t_object['retweet_count']
    f_tweet['orig_favorite_count'] = t_object['favorite_count']
    f_tweet['orig_user_id_str'] = t_object['user']['id_str']
    f_tweet['orig_user_screen_name'] = t_object['user']['screen_name']
    f_tweet['orig_user_name'] = t_object['user']['name']
    f_tweet['orig_user_followers_count'] = t_object['user']['followers_count']
    f_tweet['orig_user_friends_count'] = t_object['user']['friends_count']
    f_tweet['orig_user_statuses_count'] = t_object['user']['statuses_count']
    if 'entities' in t_object:
      if 'hashtags' in t_object['entities']:  
        # hashtags are arrays
        f_tweet['orig_hashtags'] = t_object['entities']['hashtags']
      if 'user_mentions' in t_object['entities']:
        # user_mentions are arrays
        f_tweet['orig_user_mentions'] = t_object['entities']['user_mentions']
      if 'urls' in t_object['entities']:  
        # urls are arrays
        f_tweet['orig_urls'] = t_object['entities']['urls']
      if 'media' in t_object['entities']:
        # media is an array
        for i in range(len(t_object['entities']['media'])):
          if 'source_status_id_str' in t_object['entities']['media'][i]:  
            f_tweet['orig_source_status_id_str'] = t_object['entities']['media'][i]['source_status_id_str']
    for i in range(len(t_object['extended_entities']['media'])):
      if t_object['extended_entities']['media'][i]['type'] == 'video':
        f_tweet['orig_media_type'] = t_object['extended_entities']['media'][i]['type']
        f_tweet['orig_video_length_ms'] = t_object['extended_entities']['media'][i]['video_info']['duration_millis']
        f_tweet['orig_video_url'] = t_object['extended_entities']['media'][i]['expanded_url']
        # Could potentially store the thumbnail. I think you have to access it with <url>:thumb
        # or something like that.
        #f_tweet['orig_video_thumb'] = ""
    
    f_tweet = { 'tweet' : f_tweet }
    f_tweet.update({'ID': orig_id_str})

    return f_tweet
