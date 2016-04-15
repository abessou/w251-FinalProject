from flask import render_template, flash, redirect, session, url_for, request, g
from app import app
from get_top_videos import MongoDBServices
from werkzeug.contrib.cache import SimpleCache
from youtube_viewcount import viewcount_plot
from facebook_likes import likes_plot
from twitter_favorites_count import favorites_plot
from .forms import YoutubeForm
from .forms import FacebookForm
from .forms import TwitterForm
from .forms import VIDEO_CATEGORY_TOP
from .forms import VIDEO_CATEGORY_PREDICTED


TOP_COUNT = 10

db_services = MongoDBServices(app.config['DB_SERVICES'])
cache = SimpleCache()

def get_youtube(predictions=False):    
    youtube_videos = db_services.get_top_youtube_videos(TOP_COUNT, predictions)
    return [item for item in youtube_videos]    

def get_facebook(predictions=False):    
    facebook_videos = db_services.get_top_facebook_videos(TOP_COUNT, predictions)
    return [item for item in facebook_videos]    

def get_twitter(predictions=False):    
    twitter_videos = db_services.get_top_twitter_videos(TOP_COUNT, predictions)
    return [item for item in twitter_videos]    


@app.route('/', methods=['GET', 'POST'])
@app.route('/youtube/top', methods=['GET', 'POST'])
@app.route('/youtube/top/<videoid>', methods=['GET', 'POST'])
def youtube_top(videoid=None):
            
    script, div = None, None
        
    # We may need to know the category on display.
    form = YoutubeForm()
    
    # Determine the query requested by the user (Current Top N or Predictions)
    if form.validate_on_submit():
        predictions = form.video_categories.data
        return redirect('youtube/'+predictions)
    youtube = get_youtube(predictions=False)

    # In case the user selected a video for details
    if not videoid is None:
        viewcount_history = db_services.get_youtube_viewcount_history(videoid)
        plot = viewcount_plot(viewcount_history)
        script, div = plot.get_figure()
    
    return render_template("youtube.html",
                            title='Youtube Billboard',
                            content_header='Top Youtube Videos',
                            base_url=url_for('youtube_top'),
                            form=form,
                            youtube = youtube,
                            plot_script=script, plot_div=div)        



@app.route('/youtube/predictions', methods=['GET', 'POST'])
@app.route('/youtube/predictions/<videoid>', methods=['GET', 'POST'])
def youtube_predictions(videoid=None):
            
    script, div = None, None
        
    # We may need to know the category on display.
    form = YoutubeForm()
    
    # Determine the query requested by the user (Current Top N or Predictions)
    if form.validate_on_submit():
        predictions = form.video_categories.data
        return redirect('youtube/'+predictions)

    youtube = get_youtube(predictions=True)

    # In case the user selected a video for details
    if not videoid is None:
        viewcount_history = db_services.get_youtube_viewcount_history(videoid)
        plot = viewcount_plot(viewcount_history)
        script, div = plot.get_figure()
    
    return render_template("youtube.html",
                            title='Youtube Billboard',
                            content_header='Predicted Youtube Videos',
                            base_url=url_for('youtube_predictions'),  
                            form=form,
                            youtube = youtube,
                            plot_script=script, plot_div=div)    


@app.route('/facebook/top', methods=['GET', 'POST'])
@app.route('/facebook/top/<pagelink>', methods=['GET', 'POST'])
def facebook_top(pagelink=None):
        
    script, div = None, None
    
    # We may need to know the category on display.
    form = FacebookForm()

    # Determine the query requested by the user (Current Top N or Predictions)
    if form.validate_on_submit():
        predictions = form.video_categories.data
        return redirect('facebook/'+predictions)
    facebook = get_facebook(predictions=False)
    
    # In case the user selected a video for details    
    if not pagelink is None:
        likes_history = db_services.get_facebook_likes_history(pagelink)
        plot = likes_plot(likes_history)
        script, div = plot.get_figure()
            
    return render_template("facebook.html",
                            title='Facebook Billboard',
                            content_header='Top Facebook Videos',
                            base_url=url_for('facebook_top'),
                            form=form,
                            facebook=facebook,
                            plot_script=script, plot_div=div)


@app.route('/facebook/predictions', methods=['GET', 'POST'])
@app.route('/facebook/predictions/<pagelink>', methods=['GET', 'POST'])
def facebook_predictions(pagelink=None):
    script, div = None, None

    # We may need to know the category on display.
    form = FacebookForm()
    
    # Determine the query requested by the user (Current Top N or Predictions)
    if form.validate_on_submit():
        predictions = form.video_categories.data
        return redirect('facebook/'+predictions)
    facebook = get_facebook(predictions=True)
        
    # In case the user selected a video for details
    if not pagelink is None:
        likes_history = db_services.get_facebook_likes_history(pagelink)
        plot = likes_plot(likes_history)
        script, div = plot.get_figure()
            
    return render_template("facebook.html",
                            title='Facebook Billboard',
                            content_header=('Predicted Facebook Videos'),
                            base_url=url_for('facebook_predictions'),
                            form=form,
                            facebook = facebook,
                            plot_script=script, plot_div=div)


@app.route('/twitter/top', methods=['GET', 'POST'])
@app.route('/twitter/top/<video_url>', methods=['GET', 'POST'])
def twitter_top(video_url=None):
    script, div = None, None

    # We may need to know the category on display.
    form = TwitterForm()

    # Determine the query requested by the user (Current Top N or Predictions)
    if form.validate_on_submit():
        predictions = form.video_categories.data
        return redirect('twitter/'+predictions)
    twitter = get_twitter(predictions=False)

    if not video_url is None:
        favorites_history = db_services.get_twitter_retweet_history(video_url)
        plot = favorites_plot(favorites_history)
        script, div = plot.get_figure()
        
    return render_template("twitter.html",
                            title='Twitter Billboard',
                            content_header='Top Twitter Videos',
                            base_url=url_for('twitter_top'),
                            form=form,
                            twitter = twitter,
                            plot_script=script, plot_div=div)


@app.route('/twitter/predictions', methods=['GET', 'POST'])
@app.route('/twitter/predictions/<video_url>', methods=['GET', 'POST'])
def twitter_predictions(video_url=None):
    script, div = None, None

    # We may need to know the category on display.
    form = TwitterForm()

    # Determine the query requested by the user (Current Top N or Predictions)
    if form.validate_on_submit():
        predictions = form.video_categories.data
        return redirect('twitter/'+predictions)

    twitter = get_twitter(predictions=True)
    
    # In case the user selected a video for details    
    if not video_url is None:
        favorites_history = db_services.get_twitter_retweet_history(video_url)
        plot = favorites_plot(favorites_history)
        script, div = plot.get_figure()
        
    return render_template("twitter.html",
                            title='Twitter Billboard',
                            content_header='Predicted Twitter Videos',
                            base_url=url_for('twitter_predictions'),
                            form=form,
                            twitter = twitter,
                            plot_script=script, plot_div=div)
