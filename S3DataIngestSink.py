import boto
import datetime
import uuid
import json
import sys

class S3DataIngestSink:
  """Output data to S3"""

  def __init__(self, config):
    self.config = config
    aws_access_key_id = self.config['aws_access_key_id']
    aws_secret_access_key = self.config['aws_secret_access_key']
    aws_bucket = self.config['aws_bucket']

    conn = boto.connect_s3(aws_access_key_id, aws_secret_access_key)
    
    self.bucket = conn.get_bucket(aws_bucket, validate = True)

    now = datetime.datetime.utcnow().isoformat()

    unique = str(uuid.uuid4())

    self.folder = now + '-' + unique

    print '[S3] Writing to bucket ' + aws_bucket + ' folder ' + self.folder


  def write(self, source):

    self.record_index = 0
    self.file_index = 0

    if 'batch_size' in self.config:
      self.batch_size = int(self.config['batch_size'])
    else:
      self.batch_size = 50

    self.batch = [ ]

    for item in source:
      self.record_index = self.record_index + 1
      self.batch.append(item)
      sys.stdout.write('.') # write a record indicator to stdout
      sys.stdout.flush()

      if self.record_index >= self.batch_size:
        self.flush()

    self.flush()

  def flush(self):

    filename = str(self.file_index).zfill(7)
    key_name = self.folder + '/' + filename
    key = self.bucket.new_key(key_name)
    key.set_contents_from_string(json.dumps(self.batch))

    self.file_index = self.file_index + 1

    self.batch = [ ]
    self.record_index = 0

    print('|') # Write a batch separator with a newline to stdout
    

    

    