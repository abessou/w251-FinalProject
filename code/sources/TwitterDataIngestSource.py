import sys
from itertools import ifilter
from requests_oauthlib import OAuth1Session
import json

class TwitterDataIngestSource:
  """Ingest data from Twitter"""

  def __init__(self, config):
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
    return { 'tweet' : json.loads(self.source_iterator.next()) }
