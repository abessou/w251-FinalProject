#!/bin/bash

yum install wget bzip2
wget http://repo.continuum.io/archive/Anaconda2-2.5.0-Linux-x86_64.sh
bash ./Anaconda2-2.5.0-Linux-x86_64.sh -b -p /usr/local/anaconda2/
echo 'export PATH=/usr/local/anaconda2/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
conda install pip
yum install git
git clone https://github.com/abessou/w251-FinalProject.git


