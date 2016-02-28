# -*- coding: utf-8 -*-
"""
Created on Sun Nov 15 23:17:45 2015

@author: Maximus
Pulling data from Youtube
"""

import json
import urllib

# https://www.googleapis.com/youtube/v3/videos?part=statistics&id=Q5mHPo2yDG8&key=YOUR_API_KEY
api_key = "AIzaSyCPGlBmzySKUuEId0C9GS0jrKZfArvSk6M"
service_url = 'https://www.googleapis.com/youtube/v3/videos?'


part="snippet,statistics&chart=mostPopular&fields=items(id,snippet(title),statistics)&maxResults=2&regionCode=US&key="+api_key


url = "https://www.googleapis.com/youtube/v3/videos?part=" + part


topic = json.loads(urllib.urlopen(url).read())