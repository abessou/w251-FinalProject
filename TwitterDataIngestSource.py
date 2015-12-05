import sys
import tweepy

class TwitterDataIngestSource:
  """Ingest data from Twitter"""

  def __init__(self, config):
    self.config = config

  def __iter__(self):
    auth = tweepy.OAuthHandler(
      self.config['consumer_key'], 
      self.config['consumer_secret']
    )

    auth.set_access_token(
      self.config['access_token'], 
      self.config['access_token_secret']
    )

    api = self.api = tweepy.API(auth)
    self.public_tweets = api.home_timeline()
    self.public_tweet_index = 0

    return self

  def next(self):
    if self.public_tweet_index < len(self.public_tweets):
      tweet = self.public_tweets[ self.public_tweet_index ] 
      self.public_tweet_index = self.public_tweet_index + 1
      return { 'tweet' : tweet._json }
    else:
      raise StopIteration()



