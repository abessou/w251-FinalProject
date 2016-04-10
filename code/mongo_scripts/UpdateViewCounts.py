import pymongo
import datetime
import sys

host = '67.228.179.2'
port = '27017'
database = 'VideosDB'
name_coll = 'Youtube'

client = pymongo.MongoClient(host, int(port))
db = client[database][name_coll]

count = 0
yt_videos = db.find({})
for video in yt_videos:
    if len(video['items']) > 0:
        if ('stats_history' in video['items'][0]):
            last_index = len(video['items'][0]['stats_history']) -1
            video['items'][0]['statistics']['viewCount'] = int(video['items'][0]['stats_history'][last_index]['viewCount'])
        
            for stat in video['items'][0]['stats_history']:
                stat['viewCount'] = int(stat['viewCount'])
    
            updateItems = ({'_id':video['_id']},
                           {
                               '$set':{'items':video['items']}
                           })
                
            db.update_one(updateItems[0], updateItems[1])
            count = count + 1
    
    sys.stdout.write('.')
    if (count % 80) == 0:
        sys.stdout.write('\n')
    
print ('\nDone\n')