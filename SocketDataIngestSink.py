import boto
import datetime
import uuid
import json

import os
import errno
import sys

class SocketDataIngestSink:
  """Output data to Local socket"""

  def __init__(self, config):
    self.config = config

    port = self.port = self.config['port']


    print '[SocketSink] Writing to port ' + self.port


  def write(self, source):
    for item in source:
      print 'Read: ' + str(item)
    

  def write2(self, source):

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

    file_path = self.folder + '/' + filename
    with open(file_path, 'w') as output_file:
      output_file.write(json.dumps(self.batch))

    self.file_index = self.file_index + 1

    self.batch = [ ]
    self.record_index = 0

    print('|') # Write a batch separator with a newline to stdout
    

    

    