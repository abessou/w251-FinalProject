import requests
import time
import datetime

from DataSource import DataSource

class FacebookDataIngestSource(DataSource):
    """Ingest data from Facebook"""

    def __init__(self, config, data_store):

        self.config = config

        # Search terms
        if 'track' in self.config:
            self.track = self.config['track'].split(',')
        else:
            self.track = ['ski', 'surf', 'board']
        print "\nSearch Terms: ", self.track, "\n"

        # Set number of results to return per page
        # default to 5000 with no looping of pages
        if 'page_limit' in self.config:
            self.page_lim = self.config['page_limit']
        else:
            self.page_lim = 5000

        # Authorization token
        if 'access_token' in self.config:
            self.access_token = self.config['access_token']
        else:
            self.access_token = self.__get_access_token()
        
        # Max number of times to loop through search terms
        self.loops = 0
        if 'max_loops' in self.config:
            self.maxLoops = self.config['max_loops']
        else:
            self.maxLoops = 2
        
        
        self.pages = []
        self.currentPage = ''
        self.pageVideos = {'data': [], 'paging': {}}
        self.track_index = 0
        self.video_fields = self.__get_video_fields()
        
        
    def __iter__(self):
        return self

    def next(self):

        if len(self.pages) == 0:
            if self.track_index == len(self.track):
                # All pages and track terms exhausted
                self.loops += 1
                self.track_index = 0
                if self.loops == self.maxLoops:
                    # Max loops through search terms reached
                    raise StopIteration()

            # ------- Get a list of pages by searching with track term -------
            # Request id for pages associated to search term    
            page_fields='page&fields=id,name,username,link'
            term = self.track[self.track_index]
            self.track_index += 1

            # Define url for http request to get pages id associated to search term    
            page_request_url = 'https://graph.facebook.com/search?q=%s&type=%s&limit=%d&access_token=%s'%(term,page_fields,self.page_lim,self.access_token)
            page_response = requests.get(page_request_url)

            if 'error' in page_response.json():
                print "\n !---- ERROR IN SEARCH REQUEST ----!"
                print time.ctime()
                print page_response.json()
                raise StopIteration()
            else:
                self.pages = page_response.json()['data']

            # ----------------------------------------------------------------


        # ------- Get the videos posted on one page -------
        while len(self.pageVideos['data']) == 0:
            try:
                # Get the next set of videos from the same page
                video_url = self.pageVideos['paging']['next']
                video_response = requests.get(video_url)

            except:
                # The current page has no more videos, so look for videos in the next searched page
                self.currentPage = self.pages.pop()
                video_url = 'https://graph.facebook.com/v2.5/%s?%s&access_token=%s'%(self.currentPage['id'],self.video_fields,self.access_token)

            video_response = requests.get(video_url)

            if 'error' in video_response.json():
                print "\n !---- ERROR IN PAGE REQUEST ----!"
                print time.ctime()
                print "Status Code: ", video_response.status_code
                print video_response.json()
                raise StopIteration()

            elif 'videos' in video_response.json():
                self.pageVideos = video_response.json()['videos']
            elif 'data' in video_response.json():
                self.pageVideos = video_response.json()
            else:
                self.pageVideos = {'data': [], 'paging': {}}

            print "{:3,} videos, page id: {:17}, page name:".format(len(self.pageVideos['data']),
                                                                    self.currentPage['id']),self.currentPage['name'].encode('utf-8')
        # -------------------------------------------------

        # ------- Format and Return Video Information -------
        # Take one video, transform it and return it
        return self.__format_video(self.pageVideos['data'].pop())
        

    def __format_video(self, video):
        ''' Format video dictionary'''
        
        now = datetime.datetime.utcnow().isoformat()
        
        # Add unique identified _id field
        video.setdefault('_id',video['id'])

        # Add page info
        video.setdefault('page', {})
        video['page'].setdefault('page_id', self.currentPage['id'])
        video['page'].setdefault('page_name', self.currentPage['name'])
        video['page'].setdefault('page_link', self.currentPage['link'])
        if 'username' in self.currentPage:
            video['page'].setdefault('page_username', self.currentPage['username'])

        # Summarize counts
        video.setdefault('total_likes',video['likes']['summary']['total_count'])
        video.setdefault('total_comments',video['comments']['summary']['total_count'])

        # Remove fields
        video.pop('likes',None)
        video['comments'].pop('paging',None)
        video['comments'].pop('summary', None)

        # Move lists one level up and out of data
        video['comments'] = video['comments']['data']
        
        # Add historic likes and comments
        video.setdefault('history',[{'timestamp' : now, 'likes': video['total_likes'], 'comments': video['total_comments']}])

        return video
        
    def __get_access_token(self):
        '''Get access key for Facebook API'''
        
        # Retrieve the consumer key and secret
        consumer_key = self.config['consumer_key']
        consumer_secret = self.config['consumer_secret']

        # Request and store authorization token from Facebook
        auth_url = 'https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id=%s&client_secret=%s'%(consumer_key,consumer_secret)
        token_req = requests.get(auth_url)

        return token_req.text.split('=')[1]

    def __get_video_fields(self):
        '''Create a string with video query for Facebook API'''
        
        video_fields=['id','length','backdated_time','created_time','title','description','content_category',
                    'embed_html','permalink_url','format']
        comment_fields=['message','like_count','comment_count']

        q = 'fields=videos.summary(1){'
        q += ','.join(field for field in video_fields)
        q += ',likes.limit(0).summary(total_count)'
        q += ',comments.limit(100).summary(total_count){'
        q += ','.join(field for field in comment_fields)
        q += '}}'
        
        return q