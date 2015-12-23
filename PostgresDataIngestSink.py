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
    self.table_name = self.config['table_name']
    self.engine = create_engine(self.connection_string)

    print(
      '[ProgresSink] Writing to connection string ' + 
      self.connection_string + ' table: ' + self.table_name
    )

    self.records_written = 0
    self.batch_size = 50


  def write(self, source):
    for item in source:
      try:
        if 'tweet' in item:
          tweet = [ item ]

          #print 'Tweet: ' + str(tweet)

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

          #print 'Read: ' + str(df)

          df.to_sql(self.table_name, self.engine, if_exists = 'append')
        elif 'post' in item:
          pass

        sys.stdout.write('.') # write a record indicator to stdout
      except:
        sys.stdout.write('*')

      sys.stdout.flush()
      self.records_written = self.records_written + 1

      if self.records_written % self.batch_size == 0:
        print('|')

      #raise ValueError('time to stop')


    

    
