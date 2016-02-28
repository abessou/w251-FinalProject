import boto
import datetime
import uuid
import json
import sys

class S3DataIngestSource:
  """Ingest data from S3"""

  def __init__(self, config):
    self.config = config

  def __iter__(self):
    aws_access_key_id = self.config['aws_access_key_id']
    aws_secret_access_key = self.config['aws_secret_access_key']
    aws_bucket = self.config['aws_bucket']
    aws_bucket_folder = self.config['aws_bucket_folder']
    
    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    self.bucket = conn.get_bucket(aws_bucket, validate = True)

    self.keys = [ 
      key_description.name 
      for key_description 
      in self.bucket.list(prefix = aws_bucket_folder) 
    ]

    if len(self.keys) < 1:
      raise ValueError('Not usable keys found in bucket')

    self.current_key = 0

    self.current_batch = None

    return self

  def next(self):
    if (self.current_batch is None) or (len(self.current_batch) < 1):
      if self.current_key >= len(self.keys):
        raise StopIteration('no more files/batches')

      self.current_batch = self.read_batch()

      self.current_key = self.current_key + 1

    if len(self.current_batch) > 0:
      return self.current_batch.pop(0)
    else:
      return { }

    raise StopIteration('not yet implemented')

  def read_batch(self):
    current_key = self.current_key
    bucket = self.bucket

    if self.current_key > len(self.keys):
      raise StopIteration('Exceeded keys')

    key_name = self.keys[current_key]
    print('Reading bucket ' + str(bucket) + ' batch ' + key_name)

    key = bucket.new_key(key_name)
    contents = key.get_contents_as_string()

    #print 'contents: ' + contents

    return json.loads(contents)