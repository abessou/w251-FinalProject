import pymongo
import datetime
import sys
import ConfigParser
import DataStore

class MongoDBDataIngestSink(DataStore.DataStore):
    """Output data to MongoDB"""
    
    def __init__(self, config):
        self.config = config
        self.now = datetime.datetime.utcnow().isoformat()
        
        # Get name of the collection where the data should be stored
        name_coll = self.config['source']
        
        # Allow for host and port overrides in the configuration of the sink
        host = 'localhost'
        port = '27017'
        if 'host' in self.config:
            host = self.config['host']
        if 'port' in self.config:
            port = self.config['port']
        
        client = pymongo.MongoClient(host, int(port))
        
        #self.db = client.VideoData[name_coll]
        self.db = client.test[name_coll]

        print '[MongoDb] Writing to ' + name_coll + ' collection'
    
    def write(self, source):

        # Make a first pass with the items to update.
        update_items = source.getUpdateItems()
        if update_items is not None:
            for item in update_items:
                self.db.update_one(item[0], item[1], upsert = True)

        # Make a second pass with the items to add.
        self.record_index = 0

        if 'batch_size' in self.config:
            self.batch_size = int(self.config['batch_size'])
        else:
            self.batch_size = 50

        self.batch = [ ]

        for item in source:
            self.record_index = self.record_index + 1
            item.update({'created_at': self.now})
            item.update({'last_modified': self.now})
            self.batch.append(item)
            sys.stdout.write('.') # write a record indicator to stdout
            sys.stdout.flush()

            if self.record_index >= self.batch_size:
                self.flush()

        self.flush()
        
        
    def flush(self):
        # if it's a tweet
        if 'tweet' in self.batch[0]:
          for json_obj in self.batch:
            if self.db.find({'tweet.orig_id_str':json_obj['tweet']['orig_id_str']}).count() == 0:
              self.db.insert_one(json_obj)
            else:
              if json_obj['tweet']['rt_info'] == []:
                print('old ' + json_obj['tweet']['orig_id_str'])                
              self.db.update_one(
                {'tweet.orig_id_str':json_obj['tweet']['orig_id_str']},
                {'$set':{'tweet.orig_retweet_count':json_obj['tweet']['orig_retweet_count'],
                 'tweet.orig_favorite_count':json_obj['tweet']['orig_favorite_count'],
                 'last_modified':self.now},
                 '$push':{'tweet.rt_info':json_obj['tweet']['rt_info'][0]}
                }
              )
        else:
          self.db.insert_many(self.batch)

        self.batch = [ ]
        self.record_index = 0

        print('|') # Write a batch separator with a newline to stdout

    def find(self, ids):
        '''returns a sequence of documents matching the ids passed into this function from the store'''
        '''ids are expected to be a dictionary of key:value pairs used a filter to retrieve the documents.'''
        '''Its is also assumed that the ids passed are unique and '''
        documents = []
        
        try:
            cursor = self.db.find(ids)
            for i in range(cursor.count()):
                documents.append(cursor[i]) 
        except TypeError as e:
            print "Arguments of improper type: %s"%(str(e))
            
        
        return documents
    
    def list (self, keys):
        '''returns a sequence of all the IDs in the store for the sources to update or inspect'''
        '''keys are a specification of keys that can be used to locate the ids from the store'''
        
        return self.db.distinct(keys)
    
        
'''
Standalone execution processing
'''
if __name__ == '__main__':
    test_defaults = {
        'source':'Youtube',
        'host':'67.228.179.2',
        'port':'27017',
    }
    
    parser = ConfigParser.ConfigParser(defaults=test_defaults)
    test_sink = MongoDBDataIngestSink(parser.defaults())
    
    id_list = test_sink.list("items.id")
    assert(len(id_list) > 0)
    
    documents = test_sink.find({'items.id':'gCzMhZaUpWA'})
    assert(len(documents) > 0)
