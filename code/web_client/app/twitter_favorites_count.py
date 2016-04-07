import pandas as pd
from bokeh.plotting import figure
from bokeh.io import output_notebook
from bokeh.embed import components
from pandas import *
import datetime
import sys

class favorites_plot:
    '''The class implements a view count plot from on a pymongo cursor of such counts over time for a single youtube video'''
    
    def __init__(self, cursor):
        '''Initializes an instance of this class from a pymongo cursor'''
        
        timeline = {}
        for item in cursor:
            for instance in item['tweet']['rt_history']:
                timeline[instance['rt_created_at']] = int(instance['orig_favorite_count'])    
                
        self.df = pd.DataFrame(timeline.items(), columns=['created_time', 'favorites'])
        self.df['created_time'] =  pd.to_datetime(self.df['created_time'])
        
    def get_figure(self):
        '''Returns a bokeh plot of the viewcount history'''
        
        fig = figure(x_axis_type="datetime", width=700, height=300)
        colors = ['#00FF00', '#0000FF', '#FFF000']
        
        fig.circle(self.df['created_time'], self.df['favorites'], color=colors[1], legend='Likes History')
        fig.title = str('Favorites History')
        fig.grid.grid_line_alpha=0.3
        fig.xaxis.axis_label = 'Date'
        fig.yaxis.axis_label = str('Favorites')
        fig.legend.orientation = "top_left"
        
        return components(fig)
    