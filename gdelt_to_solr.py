# -*- coding: utf-8 -*-

"""This program takes a gdelt csv and inserts it to solr"""

import csv
import datetime
import solr
from random import randint

############# CONFIGURATION #############
#file and folders
source = './source_files/'
destination = './output_files/'
results_filename = 'results_' + str(datetime.datetime.now()) + '.txt'
file_filter = '*.txt'
search_collection = 'gdelt'
search_server = 'http://localhost:8080/solr/' + str(search_collection)

#########################################

# Describe the file
def describe_csv (filename):
    with open(filename) as csvfile:
        dictreader = csv.DictReader(csvfile, delimiter='\t')
        print(dictreader.next()) #gives the first row
        totalrows = 0
        for row in dictreader:
            #print(row['Date'] , row['Sources'])
            # print(row['NumArticles'] , row['CAMEOEvents'])
            # print(row.keys())
            totalrows += 1
        print type(dictreader)
        print 'totalrows=' , totalrows

describe_csv('20160214141801.19018.gkg.txt')
    
#This function takes rows from a csv and ingest them to solr    #not very efficient because of 1 commit = 1 doc
def csv_to_solr (filename):
    with open(filename) as csvfile:
        f = csv.DictReader(csvfile, delimiter='\t') #read the csv
        s = solr.SolrConnection(search_server)      # Open Search server connection
        for row in f:
        ###### Use the lines below to inject results to solr
            print(row['Date'] , row['Sources'])  #just to see somethig is going on
            try:
                s.add ( Date = row['Date'] , Sources = row['Sources'])
                s.commit()           
            except:
                print 'Error'            
         
#csv_to_solr('20160214141801.19018.gkg.txt')