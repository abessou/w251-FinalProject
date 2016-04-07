from flask import render_template, flash, redirect, session, url_for, request, g
from app import app
from get_top_videos import MongoDBServices
from werkzeug.contrib.cache import SimpleCache
from youtube_viewcount import viewcount_plot
from facebook_likes import likes_plot
from twitter_favorites_count import favorites_plot

TOP_COUNT = 10

db_services = MongoDBServices(app.config['DB_SERVICES'])
cache = SimpleCache()

def get_youtube():    
    youtube_videos = db_services.get_top_youtube_videos(TOP_COUNT)
    return [item for item in youtube_videos]    

def get_facebook():    
    facebook_videos = db_services.get_top_facebook_videos(TOP_COUNT)
    return [item for item in facebook_videos]    

def get_twitter():    
    twitter_videos = db_services.get_top_twitter_videos(TOP_COUNT)
    return [item for item in twitter_videos]    



@app.route('/')
@app.route('/youtube')
@app.route('/youtube/<videoid>')
def youtube(videoid=None):
        
    youtube = cache.get('youtube')
    if youtube == None:
        cache.set('youtube', get_youtube(), timeout=None)
    
    script, div = None, None
    if not videoid is None:
        viewcount_history = db_services.get_youtube_viewcount_history(videoid)
        plot = viewcount_plot(viewcount_history)
        script, div = plot.get_figure()
        
    return render_template("youtube.html",
                            title='Youtube Billboard',
                            youtube = cache.get('youtube'),
                            plot_script=script, plot_div=div)


@app.route('/facebook')
@app.route('/facebook/<pagelink>')
def facebook(pagelink=None):
    facebook = cache.get('facebook')
    if facebook == None:
        cache.set('facebook', get_facebook(), timeout=None)
        
    script, div = None, None
    if not pagelink is None:
        likes_history = db_services.get_facebook_likes_history(pagelink)
        plot = likes_plot(likes_history)
        script, div = plot.get_figure()
            
    return render_template("facebook.html",
                            title='Facebook Billboard',
                            facebook = cache.get('facebook'),
                            plot_script=script, plot_div=div)

@app.route('/twitter')
@app.route('/twitter/<video_url>')
def twitter(video_url=None):
    twitter = cache.get('twitter')
    if twitter == None:
        cache.set('twitter', get_twitter(), timeout=None)
    
    script, div = None, None
    if not video_url is None:
        favorites_history = db_services.get_twitter_favorites_history(video_url)
        plot = favorites_plot(favorites_history)
        script, div = plot.get_figure()
        
    return render_template("twitter.html",
                            title='Twitter Billboard',
                            twitter = cache.get('twitter'),
                            plot_script=script, plot_div=div)
