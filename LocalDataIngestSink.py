import boto
import datetime
import uuid
import json

import os
import errno

class LocalDataIngestSink:
  """Output data to Local filesystem"""

  def __init__(self, config):
    self.config = config

    path = self.config['path']

    now = datetime.datetime.now().isoformat()

    unique = str(uuid.uuid4())

    self.folder = path + '/' + now + '-' + unique

    try:
        os.makedirs(self.folder)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    print '[Local] Writing to folder ' + self.folder


  def write(self, source):

    self.record_index = 0
    self.file_index = 0

    self.batch_size = 5

    self.batch = [ ]

    for item in source:
      self.record_index = self.record_index + 1
      self.batch.append(item)

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
    

    

    