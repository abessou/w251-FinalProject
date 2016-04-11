import pymongo

class MongoDBServices:
    '''A utility class to perform video reporting queries into the MongoDB store'''
    
    def __init__(self, config):
        '''Initializes an instance of the class'''
        
        # Initialize DB collections to fetch from
        self.youtube_collection = config['youtube_collection']
        self.twitter_collection = config['twitter_collection']
        self.facebook_collection = config['facebook_collection']
        
        # Initialize DB 
        if 'database' in config:
            database = config['database']
        else:
            database = 'VideosDB'
        if 'host' in config:
            host = config['host']
        else:
            host = 'localhost'
        if 'port' in config:
            port = int(config['port'])
        else:
            port = 27017
        
        client = pymongo.MongoClient(host, int(port))
        self.db = client[database]
        
    def get_top_youtube_videos(self, n):
        'Returns the top n Youtube videos from the document store based on an internal popularity rating'
        
        return self.db[self.youtube_collection].find({},
            projection=['items.statistics.viewCount', 'items.snippet.channelTitle', 'items.snippet.title', 'items.id' ],
            sort=[('items.statistics.viewCount',-1)],
            limit=n)

    def get_top_twitter_videos(self, n):
        '''Returns the top n Twitter videos from the document store based on an internal popularity rating'''

        return self.db[self.twitter_collection].find({},
            projection=['tweet.orig_retweet_count', 'tweet.orig_user_name', 'tweet.orig_text', 'tweet.orig_id_str'],
            sort=[('tweet.orig_retweet_count',-1)],
            limit=n)
    
    def get_top_facebook_videos(self, n):
        '''Returns the top n Facebook videos from the document store based on an internal popularity rating'''
        
        return self.db[self.facebook_collection].find({},
            projection=['total_likes', 'page.page_name', 'description', 'id' ],
            sort=[('total_likes',-1)],
            limit=n)
    
    def get_youtube_viewcount_history(self, videoid):
        '''Returns the view count history of a given youtube video'''
        return self.db[self.youtube_collection].find({'items.id':videoid}, 
            projection=['items.stats_history.viewCount', 'items.stats_history.timestamp'],
            sort=[('items.stats_history.timestamp', -1)])
    
    def get_facebook_likes_history(self, id):
        '''Returns the likes history of a given facebook video'''
        return self.db[self.facebook_collection].find({'id':id},
            projection=['history.likes', 'history.timestamp'],
            sort=[('history.timestamp', -1)])
    
    def get_twitter_retweet_history(self, id):
        '''Returns the favorites history of a given twitter video'''
        return self.db[self.twitter_collection].find({'tweet.orig_id_str':id},
            projection=['tweet.rt_history.orig_retweet_count', 'tweet.rt_history.rt_created_at'],
            sort=[('tweet.rt_history.rt_created_at', -1)])