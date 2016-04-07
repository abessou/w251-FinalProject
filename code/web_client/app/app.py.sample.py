from flask import Flask, render_template, request, redirect

app = Flask(__name__)




import pandas as pd
from bokeh.plotting import figure
from bokeh.io import output_notebook, show
from pandas import *
import pymongo
import datetime
import sys
import ConfigParser
from pymongo import MongoClient


client = MongoClient('mongodb://67.228.179.2:27017/')
db = client.test
fb = db['facebook_5']
yt = db['Youtube']
tw = db['twitter']


def data_extract(page):

    s = fb.find(
         { "page.page_name" : page},
         { "page.page_name":1,
           "created_time" : 1,
           "total_likes" :1,
           "total_comments" :1,
         } )
    return s


def data_dic(page,metric):

    content = data_extract(page) 
    timeline = {}
    for item in content:
        timeline[item['created_time']]=item[metric]
    publisher_metrics = [sum(timeline.values()),item['page']['page_name'],timeline]
    
    return publisher_metrics

def data_pd(page, metric):
    d = data_dic(page,metric)[2]
    df = pd.DataFrame(d.items(), columns=['created_time', metric])
    df['created_time'] =  pd.to_datetime(df['created_time'], format='%Y-%m-%dT%H:%M:%S+0000')
    df['page'] = data_dic(page,metric)[1]
    
    return df

def make_figure(pages,metric):
    
    p = figure(x_axis_type="datetime", width=700, height=300)
    colors = ['#00FF00', '#0000FF', '#FFF000']
    index = 0
    for page in pages:
        a = data_extract(page)
        b = data_dic(page,metric)
        df = data_pd(page,metric)
        p.circle(df['created_time'], df[metric], color=colors[index], legend=page)
        index +=1
    p.title = str(metric)
    p.grid.grid_line_alpha=0.3
    p.xaxis.axis_label = 'created_date'
    p.yaxis.axis_label = str(metric)
    p.legend.orientation = "top_left"
    return p
def display_graph(pages,metric):
    p = make_figure(pages,metric)
    return p

pages = ['Billy Elliot the Musical', 'Aladdin - Das Musical', 'Finding Neverland - The New Musical' ]



import jinja2
from bokeh.embed import components

template = jinja2.Template("""
<!DOCTYPE html>
<html lang="en-US">

<link
    href="http://cdn.pydata.org/bokeh/release/bokeh-0.11.1.min.css"
    rel="stylesheet" type="text/css"
>
<script 
    src="http://cdn.pydata.org/bokeh/release/bokeh-0.11.1.min.js"
></script>

<body>

    <h1>Video Metrics on Facebook</h1>
    
    <p> Three competing musicals as an example</p>
    
    {{ script }}
    
    {{ div }}

</body>

</html>
""")

from bokeh.embed import components 

plot = display_graph(pages,'total_likes')
script, div = components(plot)




@app.route('/')
def main():
  return redirect('/index')

@app.route('/index')
def index():
    return template.render(script=script, div=div)

if __name__ == '__main__':
  app.run(host='0.0.0.0')
