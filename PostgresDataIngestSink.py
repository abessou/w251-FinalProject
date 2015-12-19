import boto
import datetime
import uuid
import json

import os
import errno
import sys
import pandas as pd
import time

from sqlalchemy import create_engine

class PostgresDataIngestSink:
  """Output data to Postgres"""

  def __init__(self, config):
    self.config = config

    self.connection_string = self.config['connection_string']
    self.engine = create_engine(self.connection_string)

    print '[ProgresSink] Writing to connection string ' + self.connection_string

    self.records_written = 0


  def write(self, source):
    for item in source:
      tweet = [ item ]

      print 'Tweet: ' + str(tweet)

      createds = [time.strftime('%Y-%m-%d %H:%M:%S',
        time.strptime(str(tweet[i]['tweet']['created_at']),
            '%a %b %d %H:%M:%S +0000 %Y'))                  for i in range(len(tweet))]
      userid =   [tweet[i]['tweet']['id']                         for i in range(len(tweet))]
      texts =    [tweet[i]['tweet']['text']                       for i in range(len(tweet))]
      retweets = [tweet[i]['tweet']['retweet_count']              for i in range(len(tweet))]
      follows =  [tweet[i]['tweet']['user']['followers_count']    for i in range(len(tweet))]
      friends_count = [tweet[i]['tweet']['user']['friends_count'] for i in range(len(tweet))]
      urls =     [tweet[i]['tweet']['user']['url'] for i in range(len(tweet))]

      df = pd.DataFrame(
        {'created_at':createds, 'userid':userid, 'retweets':retweets,
          'text':texts, 'friendcount':friends_count,
                   'followers': follows, 'urls': urls},
        columns=['created_at','userid','retweets', 'text','friendcount', 'followers', 'urls']
      )

      print 'Read: ' + str(df)

      df.to_sql('data_test', self.engine, if_exists = 'append')

      self.records_written = self.records_written + 1

      #raise ValueError('time to stop')


    

    