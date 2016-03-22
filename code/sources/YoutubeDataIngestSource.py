from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.file import Storage
from oauth2client.tools import argparser, run_flow
from oauth2client.client import OAuth2WebServerFlow

import httplib2
import os
import sys

import ConfigParser
import json
import datetime

from DataSource import DataSource

class AddIterable:
    '''This class implements an iterable returned by the DataIngestSource for new items that need to be added to a data sink'''
    
    def __init__(self, config, yt_service):
        '''Initializes and instance of the AddIterable class'''
        
        self.yt_service = yt_service
        self.config = config
        
        # Provide an override to bypass paging of results
        partial_results = False
        if 'partial_results' in self.config:
            partial_results = self.config['partial_results']
        
        # Whether the instance is expected to iterate over all data or return only the 1st page of each results set.
        # This setting is is uded in testing environmments.
        self.partial_results = partial_results
         
        #Load self from the Youtube data api
        
        # Use the same default search terms as the Facebook data source
        if 'track' in self.config:
            search_terms = self.config['track']
        else:
            search_terms = ['ski', 'surf', 'board']
        
        # Retrieve the client_id from the configuration file.
        # We'll generate a default one if it's not available.
        if 'client_id' not in self.config:
            client_id = "W251FinalProject"
        else:
            client_id = self.config['client_id']
         
        nextPageToken = ''
        self.video_ids = []
        
        # Initialize the index into the collection
        self.index = 0
        
        # Load the video ids that are should be returned to the sink
        while (True):
            # Get a list of videos matching the search terms
            # Call the search.list method to retrieve results matching the specified
            # query term. Always specify videos from the 24 hrs
            if nextPageToken == '':
                search_response = self.yt_service.search().list(
                    type="video",
                    q=search_terms,
                    part="id,snippet",
                    maxResults=50,
                    publishedAfter= (datetime.datetime.today()-datetime.timedelta(days=int(self.config['days_to_fetch']))).strftime("%Y-%m-%dT%H:%M:%SZ")
                    ).execute()        
            else:
                search_response = self.yt_service.search().list(
                    type="video",
                    q=search_terms,
                    part="id,snippet",
                    pageToken = nextPageToken,
                    maxResults=50,
                    publishedAfter= (datetime.datetime.today()-datetime.timedelta(days=int(self.config['days_to_fetch']))).strftime("%Y-%m-%dT%H:%M:%SZ")
                    ).execute()        
                
            # Iterate through the returned videos. Capture all video IDs
            for item in search_response["items"]:
                self.video_ids.append(item["id"]["videoId"])

            nextPageToken = search_response['nextPageToken'] if 'nextPageToken' in search_response else ''
            if nextPageToken == '' or self.partial_results == True:
                break
        
    def next(self):
        '''Retrieve the next video from the feed that the instance was initialized'''
        '''with and query the video details'''
        
        if self.index < len(self.video_ids):
            
            video_id = self.video_ids[self.index]
            self.index += 1
            
            # Initialize the video_details instance with the Youtube video details
            video_details = self.yt_service.videos().list(part='snippet,statistics', id=video_id).execute()
            
            # Include the video ID for the sink to be able to consume
            video_details["ID"] = video_id
            
            # Start a statistics history array to accumlate stats updates. Only if the video details being downloaded
            # contain a document item.
            if len(video_details['items']) > 0:
                video_details['items'][0]['stats_history']=[]
                # Include the original statistics item into the array
                video_details['items'][0]['stats_history'].append(video_details['items'][0]['statistics'].copy())
                # Set a timestamp on the statistic history item
                video_details['items'][0]['stats_history'][0]['timestamp'] = datetime.datetime.utcnow().isoformat()
            
            next_ct_PageToken = ''
            try:
                while(True):
                    if next_ct_PageToken == '':
                        # Get the comment threads for the video
                        comment_threads = self.yt_service.commentThreads().list(part="snippet", videoId = video_id, maxResults = 50, textFormat='plainText').execute()
                    else:
                        comment_threads = self.yt_service.commentThreads().list(part="snippet", videoId = video_id, maxResults = 50, textFormat='plainText', pageToken=next_ct_PageToken).execute()
                        
                    # Attach the comment thread to the video object
                    if 'commentThreads' not in video_details:
                        video_details["commentThreads"] = comment_threads['items']
                    else:
                        video_details["commentThreads"].extend(comment_threads['items'])
                        
                    # Get all the comments in the thread and attach to the comment thread.
                    for comment_thread in comment_threads['items']: 
                        parent_id = comment_thread['id']
                        
                        next_c_page_token = ''
                        while(True):
                            if next_c_page_token == '':
                                comments = self.yt_service.comments().list(part='snippet', parentId=parent_id, textFormat='plainText', maxResults = 50).execute()
                            else:
                                comments = self.yt_service.comments().list(part='snippet', parentId=parent_id, textFormat='plainText', maxResults = 50, pageToken=next_c_page_token).execute()
                                
                            if 'comments' not in comment_thread:
                                comment_thread["comments"] = comments['items']
                            else:
                                comment_thread['comments'].extend(comments['items'])
                                
                            next_c_page_token = comments['nextPageToken'] if 'nextPageToken' in comments else ''
                            if next_c_page_token == '' or self.partial_results == True:
                                break
                    
                    # Check for the need to process the next results page
                    next_ct_PageToken = comment_threads['nextPageToken'] if 'nextPageToken' in comment_threads else ''
                    if next_ct_PageToken == '' or self.partial_results == True:
                        break                        
            except HttpError as e:
            # Some videos appear to have comments disabled and return 403 HTTP erorrs when those are requested
            # Print the exception messgae and continue processing
                print "HttpError handled retrieving video comments: %s\n" % (e.message)

            return video_details
            
        else:
            raise StopIteration
         
class UpdateIterable:
    '''This class implements an Iterable of video data to update in the data store'''
    
    def __init__(self, config, yt_service, data_store):
        '''Initializes an instance of this class'''

        self.config = config
        self.yt_service = yt_service
        self.data_store = data_store
        
        # Provide an override to bypass paging of results
        partial_results = False
        if 'partial_results' in self.config:
            partial_results = self.config['partial_results']
        
        # Whether the instance is expected to iterate over all data or return only the 1st page of each results set.
        # This setting is is uded in testing environmments.
        self.partial_results = partial_results
        
                
    def __iter__(self):
        '''Returns self as an Iterable for enumeration'''
        
        # Query the store for the list of items ids in store
        self.video_ids = self.data_store.list("items.id")
        self.index = 0
        
        return self
    
    def next(self):
        '''Return the next element in the iteration to the caller'''
        
        # Stop the iteration if we've exhausted our collection of updated items
        if self.index >= len(self.video_ids):
            raise StopIteration
        
        # Retrieve the content we want to update for every previously stored video item.
        # At the moment, we'll only update statistics. We might want to update comment threads too.
        video_id = self.video_ids[self.index]
        self.index += 1
        
        # Initialize the video_details instance with the Youtube video details
        video_details = self.yt_service.videos().list(part='statistics', id=video_id).execute()
                
        next_ct_PageToken = ''
        try:
            while(True):
                if next_ct_PageToken == '':
                    # Get the comment threads for the video
                    comment_threads = self.yt_service.commentThreads().list(part="snippet", videoId = video_id, maxResults = 50, textFormat='plainText').execute()
                else:
                    comment_threads = self.yt_service.commentThreads().list(part="snippet", videoId = video_id, maxResults = 50, textFormat='plainText', pageToken=next_ct_PageToken).execute()
                    
                # Attach the comment thread to the video object
                if 'commentThreads' not in video_details:
                    video_details["commentThreads"] = comment_threads['items']
                else:
                    video_details["commentThreads"].extend(comment_threads['items'])
                    
                # Get all the comments in the thread and attach to the comment thread.
                for comment_thread in comment_threads['items']: 
                    parent_id = comment_thread['id']
                    
                    next_c_page_token = ''
                    while(True):
                        if next_c_page_token == '':
                            comments = self.yt_service.comments().list(part='snippet', parentId=parent_id, textFormat='plainText', maxResults = 50).execute()
                        else:
                            comments = self.yt_service.comments().list(part='snippet', parentId=parent_id, textFormat='plainText', maxResults = 50, pageToken=next_c_page_token).execute()
                            
                        if 'comments' not in comment_thread:
                            comment_thread["comments"] = comments['items']
                        else:
                            comment_thread['comments'].extend(comments['items'])
                            
                        next_c_page_token = comments['nextPageToken'] if 'nextPageToken' in comments else ''
                        if next_c_page_token == '' or self.partial_results == True:
                            break
                
                # Check for the need to process the next results page
                next_ct_PageToken = comment_threads['nextPageToken'] if 'nextPageToken' in comment_threads else ''
                if next_ct_PageToken == '' or self.partial_results == True:
                    break                        
        except HttpError as e:
        # Some videos appear to have comments disabled and return 403 HTTP erorrs when those are requested
        # Print the exception messgae and continue processing
            print "HttpError handled retrieving video comments: %s\n" % (e.message)

        # Return the contents we need to update for the current document with paths in the data store
        updateItems = ({'items.id':video_id},
                       {
                           '$set': {},
                           '$push':{}
                       })
        if len(video_details['items']) > 0:
            # Overwrite the statistics item with the updated stats
            updateItems[1]['$set']['items.0.statistics']=video_details['items'][0]['statistics']
            # Add a copy of the new statistics item to the stats_history item with an updated timestamp
            stats_history_item = video_details['items'][0]['statistics'].copy()
            stats_history_item['timestamp'] = datetime.datetime.utcnow().isoformat()
            updateItems[1]['$push']['items.0.stats_history'] = stats_history_item
        if 'commentThreads' in video_details:
            updateItems[1]['$set']['commentThreads'] = video_details['commentThreads']
            
                
        return updateItems
    
        
class YoutubeDataIngestSource(DataSource):
    '''This class ingests video data from Youtube'''
    
    '''Class constants'''
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"
    
    
    def __init__(self, config, data_store):
        '''Initialize an instance of this class'''
                
        # Hold on to the configuration object
        self.config = config
        
        # Hold a reference to the data store instance
        self.data_store = data_store

        # Retrieve the developer key from the configuration file
        # It's expected to be in the config and we'll terminate if it's not
        if 'developer_key' not in self.config:
            raise "Invalid configuration. Developer key missing from Youtube configuration."
        else:
            developer_key = self.config['developer_key']
        

        # Create an instance of the service.
        self.yt_service = build(YoutubeDataIngestSource.YOUTUBE_API_SERVICE_NAME, YoutubeDataIngestSource.YOUTUBE_API_VERSION, developerKey=developer_key)
        
    def __iter__(self):
        '''Returns an Iterable for new items to add to a data sink'''
        
        # Return an instance of the AddIterable class
        return AddIterable(self.config, self.yt_service)
    
    def getUpdateItems(self):
        '''Returns an Iterable of items to udpate in the data store'''
        
        return UpdateIterable(self.config, self.yt_service, self.data_store)
    
'''
Standalone execution processing
'''    
if __name__ == '__main__':
    test_defaults = {
        'developer_key':'AIzaSyDXQgUC3yT9gzQfhf5DIpqas5tXyKCph2Q',
        'track':'surf|ski|board',
        'days_to_fetch':1,
        'partial_results': True
    }
    
    parser = ConfigParser.ConfigParser(defaults=test_defaults)
    youtube_source = YoutubeDataIngestSource(parser.defaults(), None)
    
    for video_details in youtube_source:
        json_details = json.dumps(video_details)
        print str(json_details)
    
