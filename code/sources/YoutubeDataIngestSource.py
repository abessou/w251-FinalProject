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

class YoutubeDataIngestSource:
    '''This class ingests video data from Youtube'''
    
    '''Class constants'''
    YOUTUBE_API_SERVICE_NAME = "youtube"
    YOUTUBE_API_VERSION = "v3"
    
    
    def __init__(self, config):
        '''Initialize an instance of this class'''
                
        # Hold on to the configuration object
        self.config = config

        # Retrieve the developer key from the configuration file
        # It's expected to be in the config and we'll terminate if it's not
        if 'developer_key' not in self.config:
            raise "Invalid configuration. Developer key missing from Youtube configuration."
        else:
            developer_key = self.config['developer_key']
        
        # Provide an override to bypass paging of results
        partial_results = False
        if 'partial_results' in self.config:
            partial_results = self.config['partial_results']
        
        # Whether the instance is expected to iterate over all data or return only the 1st page of each results set.
        # This setting is is uded in testing environmments.
        self.partial_results = partial_results

        # Create an instance of the service.
        self.yt_service = build(YoutubeDataIngestSource.YOUTUBE_API_SERVICE_NAME, YoutubeDataIngestSource.YOUTUBE_API_VERSION, developerKey=developer_key)
        
    def __iter__(self):
        '''Returns loads self from the Youtube data api and returns self an iterable instance'''
        
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
            self.index = 0
            for item in search_response["items"]:
                self.video_ids.append(item["id"]["videoId"])

            nextPageToken = search_response['nextPageToken'] if 'nextPageToken' in search_response else ''
            if nextPageToken == '' or self.partial_results == True:
                break
            
        return self
    
    def next(self):
        '''Retrieve the next video from the feed that the instance was initialized'''
        '''with and query the video details'''
        
        if self.index < len(self.video_ids):
            
            video_id = self.video_ids[self.index]
            self.index += 1
            
            # Initialize the video_details instance with the video Youtube details
            video_details = self.yt_service.videos().list(part='snippet,statistics', id=video_id).execute()
            
            # Include the video ID for the sink to be able to consume
            video_details["ID"] = video_id
            
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
    youtube_source = YoutubeDataIngestSource(parser.defaults())
    
    for video_details in youtube_source:
        json_details = json.dumps(video_details)
        print str(json_details)
    
