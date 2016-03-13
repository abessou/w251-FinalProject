#!/bin/bash

pushd ~
mkdir YoutubeApi
cd YoutubeApi
git clone https://github.com/google/gdata-python-client.git
cd gdata-python-client/
python ./setup.py install 
popd
pip install tlslite
$ pip install --upgrade google-api-python-client
