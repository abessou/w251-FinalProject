import pymongo
import datetime
import sys

class MongoDBDataIngestSink:
    """Output data to MongoDB"""
    
    def __init__(self, config):
        self.config = config
        self.now = datetime.datetime.utcnow().isoformat()
        
        # Get name of the collection where the data should be stored
        name_coll = self.config['source']
        
        client = pymongo.MongoClient()
        
        #self.db = client.VideoData[name_coll]
        self.db = client.test[name_coll]

        print '[MongoDb] Writing to ' + name_coll + ' collection'
    
    def write(self, source):

        self.record_index = 0

        if 'batch_size' in self.config:
            self.batch_size = int(self.config['batch_size'])
        else:
            self.batch_size = 50

        self.batch = [ ]

        for item in source:
            self.record_index = self.record_index + 1
            item.update({'created_at': self.now})
            self.batch.append(item)
            sys.stdout.write('.') # write a record indicator to stdout
            sys.stdout.flush()

            if self.record_index >= self.batch_size:
                self.flush()

        self.flush()

        
    def flush(self):
        self.db.insert_many(self.batch)

        self.batch = [ ]
        self.record_index = 0

        print('|') # Write a batch separator with a newline to stdout
