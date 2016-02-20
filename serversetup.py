#!/usr/bin/env python

# Setting up the environment

import subprocess
import sys
import os

#List commands to execute here.

#Server setup
UPDATE = "sudo apt-get update"
GIT = "sudo apt-get install git"
PIP = "sudo apt-get install python-pip"
JAVA = "sudo apt-get install openjdk-7-jre"

#Solr install and start
GETSOLR = "wget http://mirror.cc.columbia.edu/pub/software/apache/lucene/solr/5.4.0/solr-5.4.0.tgz"
UNPAKSOLR = "tar -xvf solr-5.4.0.tgz"
STARTSOLR = "bin/solr start -p $PORT"
CREATE_COLLECTION = "bin/solr create -c linksdgs"


#Python stuff
NLTK = "sudo pip install -U nltk"
SCIKIT_LEARN = "sudo pip install -U scikit-learn"
PDFMINER = "sudo pip install -U pdfminer"
PUNKT = "sudo python -m nltk.downloader punkt"
SOLRPY = "sudo pip install -U solrpy"

#order commands in sequence ## Uncomment these for 1st install
cmds = [
    #UPDATE,
    GIT,
    PIP,
    JAVA,
    #GETSOLR,
    #UNPAKSOLR, 
    ]


#more commands in sequence ## Comment Creata_collection after the first install
cmds2 = [ 
    #STARTSOLR,
    #CREATE_COLLECTION
    ]


print os.getcwd()

#Iterates over list, running statements for each item in the list
count=0
for cmd in cmds:
    count+=1
    print "Running Command Number %s" % count
    subprocess.call(cmd, shell=True)
    
#os.chdir('/home/ubuntu/workspace/solr-5.4.0/')

count2=0
for cmd in cmds2:
    count2+=1
    print "Running Command Number %s" % count2
    subprocess.call(cmd, shell=True)