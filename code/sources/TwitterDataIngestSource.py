import sys
from itertools import ifilter
from requests_oauthlib import OAuth1Session
from requests.exceptions import ChunkedEncodingError
import json
import time
import datetime

from DataSource import DataSource

def isVideo(tweet):
  """Returns True if a tweet had a video uploaded with it, false otherwise."""

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
        filtered_tweet = self.processTweet(next_tweet)
        if filtered_tweet != {}:
          break
      # could put more error handling in here to handle HTTP errors and
      # disconnection errors
      except ChunkedEncodingError:
        print('Chunked Encoding Error next')
        self.__iter__()
        continue
    return filtered_tweet
  
  def getUpdateItems(self):
    """ Returns a copy of the list of items that need to be updated in the 
      database vs inserted, None otherwise.  Right now the self.update_items
      list is not being updated so this method always returns None.
    """
    if self.update_items == []:
      return None
    else:
      list_copy = list(self.update_items)
      self.update_items = []
      return list_copy
      
  def updateRetweet(self, id_str, history):
    """ Update the tweet with ID id_str with information from history."""
    
    # Create the update according to the structure of a tweet.  Update
    # the retweet_count and favorite_count for this retweet.  Push the 
    # history information into the rt_history array.
    update = ({'ID':id_str},
              {'$set':{'tweet.orig_retweet_count':history['orig_retweet_count'],
              'tweet.orig_favorite_count':history['orig_favorite_count'],
              'tweet.orig_user_followers_count':history['orig_user_followers_count'],
              'tweet.orig_user_friends_count':history['orig_user_friends_count'],
              'tweet.orig_user_statuses_count':history['orig_user_statuses_count'],
              'tweet.orig_user_favourites_count':history['orig_user_favourites_count'],
              'last_modified':datetime.datetime.utcnow().isoformat()},
              '$push':{'tweet.rt_history':history}
              })
    # Update the database with this update
    self.data_store.update_one(update) 

  def updateReplyTweet(self, id_str, history):
    """ Update the tweet with ID id_str with information from history."""
    
    # Create the update according to the structure of a tweet.
    # Push the history information into the reply_history array.
    update = ({'ID':id_str},
              {'$set':{'last_modified':datetime.datetime.utcnow().isoformat()},
              '$inc':{'tweet.reply_count':1},
              '$push':{'tweet.reply_history':history}
              })
    # Update the database with this update
    self.data_store.update_one(update) 

  def processTweet(self, tweet):
    """ Processes tweet and returns {} if the tweet is a retweet and already in 
      the database.  In this case the tweet is updated directly in the database.
      If the tweet is a new tweet then build the object f_tweet with the
      relevant tweet information that needs to be stored.
    """
    # Initialize variables    
    f_tweet = {}
    f_tweet['retweet'] = 0
    f_tweet['reply_count'] = 0
    f_tweet['rt_history'] = []      
    f_tweet['reply_history'] = []      
    # If the tweet is a retweet it will have a 'retweeted_status' object in it.
    if 'retweeted_status' in tweet:
      # Set t_object to be the nested object within the retweet that contains
      # all of the original tweet information.
      t_object = tweet['retweeted_status']
      # If it's a video proceed, otherwise return {}
      if isVideo(t_object):      
        # initialize a history dictionary and set it with all of the
        # retweet information that is being stored.
        history = {}
        history['rt_id_str'] = tweet['id_str']
        history['rt_created_at'] = time.strftime('%Y-%m-%dT%H:%M:%S',
          time.strptime(str(tweet['created_at']),'%a %b %d %H:%M:%S +0000 %Y'))
        history['rt_text'] = tweet['text']
        history['orig_retweet_count'] = t_object['retweet_count']
        history['orig_favorite_count'] = t_object['favorite_count']
        history['orig_user_followers_count'] = t_object['user']['followers_count']
        history['orig_user_friends_count'] = t_object['user']['friends_count']
        history['orig_user_statuses_count'] = t_object['user']['statuses_count']
        history['orig_user_favourites_count'] = t_object['user']['favourites_count']
        # If the tweet is in the database, then return {} and update the tweet
        # in the database
        orig_id_str = t_object['id_str']
        updates = self.data_store.find({'ID':orig_id_str})
        if updates != []:
          self.updateRetweet(orig_id_str, history)
          return {}
        else:
          f_tweet['retweet'] = 1
          f_tweet['rt_history'] = [history]
      else:
        return {}
    # If the tweet is a reply.  If we are storing the original tweet then
    # update the reply information.  If we are not storing the original tweet
    # then return {}.
    elif 'in_reply_to_status_id_str' in tweet and tweet['in_reply_to_status_id_str'] is not None:
      orig_id_str = tweet['in_reply_to_status_id_str']
      updates = self.data_store.find({'ID':orig_id_str})
      if updates != []:
        print 'Reply: ' + orig_id_str
        # initialize a history dictionary and set it with all of the
        # reply information that is being stored.
        history = {}
        history['reply_id_str'] = tweet['id_str']
        history['reply_created_at'] = time.strftime('%Y-%m-%dT%H:%M:%S',
          time.strptime(str(tweet['created_at']),'%a %b %d %H:%M:%S +0000 %Y'))
        history['reply_text'] = tweet['text']
        self.updateReplyTweet(orig_id_str, history)
      return {}
    else:
      # It's an original tweet.
      # If it's a video, then store it, otherwise return {}
      if isVideo(tweet):      
        t_object = tweet
        orig_id_str = t_object['id_str']
      else:
        return {}
    
    # Store the desired tweet information in the f_tweet object.
    f_tweet['orig_tweet_object'] = t_object
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
    f_tweet['orig_user_favourites_count'] = t_object['user']['favourites_count']
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
        f_tweet['orig_video_expanded_url'] = t_object['extended_entities']['media'][i]['expanded_url']
        f_tweet['orig_video_display_url'] = t_object['extended_entities']['media'][i]['display_url']
        f_tweet['orig_video_url'] = t_object['extended_entities']['media'][i]['url']
    
    # add the key tweet before all tweets
    f_tweet = { 'tweet' : f_tweet }
    # add an ID field to the f_tweet dictionary that is the unique ID string
    # for the tweet.
    f_tweet.update({'ID': orig_id_str})

    return f_tweet
