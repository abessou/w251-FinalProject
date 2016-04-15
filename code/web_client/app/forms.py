from flask.ext.wtf import Form
from wtforms import SelectField
from wtforms.validators import DataRequired

VIDEO_CATEGORY_TOP = 'top'
VIDEO_CATEGORY_PREDICTED = 'predictions'
VIDEO_CATEGORY_CHOICES = [(VIDEO_CATEGORY_TOP,'Current Top Popularity'),(VIDEO_CATEGORY_PREDICTED,'Predicted to be Popular')]
class YoutubeForm(Form):
    '''The form class for youtube videos'''
    video_categories = SelectField('Video Categories', choices=VIDEO_CATEGORY_CHOICES)

class FacebookForm(Form):
    '''The form class for youtube videos'''
    video_categories = SelectField('Video Categories', choices=VIDEO_CATEGORY_CHOICES)

class TwitterForm(Form):
    '''The form class for youtube videos'''
    video_categories = SelectField('Video Categories', choices=VIDEO_CATEGORY_CHOICES)
    