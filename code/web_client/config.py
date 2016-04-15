import os
basedir = os.path.abspath(os.path.dirname(__file__))

WTF_CSRF_ENABLED = True
SECRET_KEY = 'take-a-guess'

DB_SERVICES = {
    'youtube_collection':'Youtube',
    'twitter_collection':'twitter',
    'facebook_collection':'facebook',
    'host':'67.228.179.2',
    'port':'27017',
    'database':'VideosDB',
}