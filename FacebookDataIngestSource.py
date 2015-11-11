import requests
import json

class FacebookDataIngestSource:
  """Ingest data from Facebook"""

  def __init__(self, config):
    self.config = config
    
    
  
  def __iter__(self):
#### Retrieve the consumer key and secret
    consuner_key = self.config['consumer_key']
    consumer_secret = self.config['consumer_secret']  
#### Define url for http request for access token
    auth_url = 'https://graph.facebook.com/oauth/access_token?grant_type=client_credentials&client_id=%s&client_secret=%s'%(consumer_key,consumer_secret)
#### Get authorization token from Facebook and store it for future use
    token_req = requests.get(auth_url)
    self.access_token = token_req.text.split('=')[1]
    
#### Retrieve term to search    
    page_search_term = config.terms = [ '#kitesurfing' , '#basejumping' ]

#### Request id for pages associated to search term    
    page_search_request='page&fields=id'

#### Define url for http request to get pages id associated to search term    
    page_search_url = 'https://graph.facebook.com/search?q=%s&type=%s&access_token=%s'%(page_search_term, page_search_request, self.access_token)

#### Get a list of pages id associated to search term    
    self.page_search = requests.get(search_url)
    self.page_json = self.page_search.json()
#### Initialize page index to zero 
    self.page_index = 0
    self.index = 0

    return self

  def next(self):
#### Request all videos associated to each of the pages found to be relevant for the search
    if self.page_index < len(page_json['data']):
      page_id = page_json['data'][self.page_index]['id']
      self.page_index = self.page_index + 1
      video_url = 'https://graph.facebook.com/v2.5/%s/videos?&fields=permalink_url,sharedposts,likes,comments&access_token=%s'%(page_id,self.access_token)
      video_search = requests.get(video_url)
      video_json = video_search.json()
      video_index = 0
          if video_index < len(video_json['data']):
              rows = video_json['data'][video_index])
              self.rows[self.index] = rows
              self.index = self.index + 1 
              
    return self.rows    
    else:
      raise StopIteration()
