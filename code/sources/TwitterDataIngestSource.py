import sys
from itertools import ifilter
from requests_oauthlib import OAuth1Session
import json
import time

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

def filterTweet(tweet):
  f_tweet = {}
  if 'retweeted_status' in tweet:
    f_tweet['retweet'] = 1
    #??do i need to convert this to a dictionary? i don't think so
    t_object = tweet['retweeted_status']
    f_tweet['rt_id_str'] = tweet['id_str']
    f_tweet['rt_created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',
      time.strptime(str(tweet['created_at']),'%a %b %d %H:%M:%S +0000 %Y'))
    f_tweet['rt_text'] = tweet['text']
  else:
    f_tweet['retweet'] = 0
    t_object = tweet
    f_tweet['rt_id_str'] = ""
    f_tweet['rt_created_at'] = ""
    f_tweet['rt_text'] = ""
    
  f_tweet['orig_id_str'] = t_object['id_str']
  f_tweet['orig_created_at'] = time.strftime('%Y-%m-%d %H:%M:%S',
    time.strptime(str(t_object['created_at']),'%a %b %d %H:%M:%S +0000 %Y'))
  f_tweet['orig_text'] = t_object['text']
  f_tweet['orig_retweet_count'] = t_object['retweet_count']
  f_tweet['orig_favorite_count'] = t_object['favorite_count']
  f_tweet['orig_user_id_str'] = t_object['user']['id_str']
  f_tweet['orig_user_screen_name'] = t_object['user']['screen_name']
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
  return f_tweet

class TwitterDataIngestSource(DataSource):
  """Ingest data from Twitter"""

  def __init__(self, config, data_store):
    self.config = config

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
    # This is a dictionary
    next_tweet = json.loads(self.source_iterator.next())
    while not isVideo(next_tweet):
      next_tweet = json.loads(self.source_iterator.next())
    filtered_tweet = filterTweet(next_tweet)
  
    return { 'tweet' : filtered_tweet }

