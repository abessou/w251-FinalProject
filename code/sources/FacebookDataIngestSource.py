
import sys
from itertools import ifilter
import requests
import json
from pprint import pprint

class FacebookDataIngestSource:
    """Ingest data from Facebook"""

    def __init__(self, config):
        self.config = config
        
        if 'track' in self.config:
            self.track = self.config['track']
        else: 
            self.track = ['ski', 'surf', 'board']
        
        
        if 'page_limit' in self.config:
            self.page_lim = self.config['page_limit']
        else: 
            self.page_lim = 5000
            
        if 'access_token' in self.config:
            self.access_token = self.config['access_token']
        else:
            # Retrieve the consumer key and secret
            consumer_key = self.config['consumer_key']
            consumer_secret = self.config['consumer_secret']

            # Request and store authorization token from Facebook
            auth_url = 'https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id=%s&client_secret=%s'%(consumer_key,consumer_secret)
            token_req = requests.get(auth_url)

            self.access_token = token_req.text.split('=')[1]
            
        
    def __iter__(self):
        self.pages = []
        self.currentPage = ''
        self.pageVideos = {'data': []}
        self.track_index = 0
        self.video_fields = get_video_fields()
        

        
        return self

    def next(self):
            
        if len(self.pages) == 0:
            
            if self.track_index == len(self.track):
                # All pages and track terms exhausted
                raise StopIteration()
            
            # ------- Get a list of pages by searching with track term -------
            # Request id for pages associated to search term    
            page_fields='page&fields=id,name'
            
            term = self.track[self.track_index]
            self.track_index += 1
        
            # Define url for http request to get pages id associated to search term    
            page_request_url = 'https://graph.facebook.com/search?q=%s&type=%s&limit=%d&access_token=%s'%(term,page_fields,self.page_lim,self.access_token)
            page_response = requests.get(page_request_url)

            self.pages = page_response.json()
            self.pages = self.pages['data']
            # ----------------------------------------------------------------
        
        
        while len(self.pageVideos['data']) == 0:  
        #if len(self.pageVideos['data']) == 0:  
            # Currently no videos stored. Get videos for page
            self.currentPage = self.pages.pop()
            print self.currentPage

            video_url = 'https://graph.facebook.com/v2.5/%s?%s&access_token=%s'%(self.currentPage['id'],self.video_fields,self.access_token)
            video_response = requests.get(video_url)
            
            self.pageVideos = video_response.json()
            self.pageVideos = self.pageVideos['videos']
            
        # Take one video, transform it and return it

        video = self.pageVideos['data'].pop()
        
        # Add fields
        video.setdefault('page_name', video['comments']['summary']['total_count'])
        video.setdefault('total_likes',video['likes']['summary']['total_count'])
        video.setdefault('total_comments',video['comments']['summary']['total_count'])
        
        # Remove Fields
        video.pop('likes',None)
        video['comments'].pop('paging',None)
        video['comments'].pop('summary', None)
        
        # Move lists one level up and out of data
        video['comments'] = video['comments']['data']
        
        return video
        
            
def get_video_fields():
    video_fields=['id','length','backdated_time','created_time','title','description','content_category',
                'copyrighted','embed_html','permalink_url','format']
    comment_fields=['message','like_count','comment_count']

    q = 'fields=videos.limit(50).summary(1){'
    q += ','.join(field for field in video_fields)
    q += ',likes.limit(0).summary(total_count)'
    q += ',comments.limit(100).summary(total_count){'
    q += ','.join(field for field in comment_fields) 
    q += '}}'
    
    print "\n",q,"\n"
    return q
