import boto
import datetime
import uuid
import json
import sys

import glob

class LocalDataIngestSource:
  """Ingest data from Local"""

  def __init__(self, config):
    self.config = config

  def __iter__(self):
    self.path = self.config['path']

    self.keys = glob.glob(self.path + '/*')

    if len(self.keys) < 1:
      raise ValueError('No usable files found in directory: ' + self.path)

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

    if self.current_key > len(self.keys):
      raise StopIteration('Exceeded keys')

    key_name = self.keys[current_key]
    print('Reading file ' + key_name)

    with open(key_name) as data_file:
      batch = json.load(data_file)

    return batch