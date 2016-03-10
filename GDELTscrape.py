# -*- coding: utf-8 -*-
"""
Created on Thu Mar 10 09:52:45 2016

@author: Yilin.Wei
Based on script of @author: patrickstocklin
"""

import os
from datetime import date, timedelta

import zipfile
import requests
import csv
import numpy as np
import pandas as pd

'''
====================================================================
Scraper to download a '.CSV.zip' (actually tab-delimited) from GDELT's Daily Event Upload Repo.
Then uploads JSON to XDATA Kafka as Topic
========================================================================================
'''

'''
========================================================================================
GDELT addr: http://data.gdeltproject.org/events/index.html

Format of File : YYYYMMDD.export.CSV.zip (##.#MB) (MD5: @@@@@@@@@@@@@@@@)
File: YYYYMMDD.export.CSV ===> Tab-Delimited File Containing News Events separated by \n

58 attributes per line:
===========
GlobalEventID
Day
MonthYear
Year
FractionDate
===========
Actor1Code
Actor1Name
Actor1CountryCode
Actor1KyesterdaynGroupCode
Actor1EthnicCode
Actor1Religion1Code
Actor1Religion2Code
Actor1Type1Code
Actor1Type2Code
Actor1Type3Code
Actor2Code
Actor2Name
Actor2CountryCode
Actor2KyesterdaynGroupCode
Actor2EthnicCode
Actor2Religion1Code
Actor2Religion2Code
Actor2Type1Code
Actor2Type2Code
Actor2Type3Code
===========
IsRootEvent
EventCode
EventBaseCode
EventRootCode
QuadClass
GoldsteinScale
NumMentions
NumSources
NumArticles
AvgTone
===========
Actor1Geo_Type
Actor1Geo_Fullname
Actor1Geo_CountryCode
Actor1Ger_ADM1Code
Actor1Geo_Lat
Actor1Geo_Long
Actor1Geo_FeatureID
Actor2Geo_Type
Actor2Geo_Fullname
Actor2Geo_CountryCode
Actor2Ger_ADM1Code
Actor2Geo_Lat
Actor2Geo_Long
Actor2Geo_FeatureID
ActionGeo_Type
ActionGeo_Fullname
ActionGeo_CountryCode
ActionGer_ADM1Code
ActionGeo_Lat
ActionGeo_Long
ActionGeo_FeatureID
===========
DATEADDED
SOURCEURL
===========
Total: 58 Fields

========================================================================================
'''
# Convert date format to number format
def date2num(date):
    mth = ''
    day = ''
    if date.month < 10: 
        mth = '0' + str(date.month) 
    else:
        mth = str(date.month)
    if date.day < 10:
        day = '0' + str(date.day)
    else:
        day = str(date.day)
    num_date = int(str(date.year) + mth + day)
    return num_date

# Download data
def download(datelist):
    url = "http://data.gdeltproject.org/events/"
    fileForm = ".export.CSV.zip"
    for i in datelist:
        print("Downloading %d .zip" %i)
        r = requests.get(url + str(i) + fileForm)
        with open("target.zip", "wb") as code:
            code.write(r.content)
        print("Completed Download")
        with zipfile.ZipFile('target.zip', 'r') as z:
            z.extractall()
        print("Completed Extraction")
        
        
# Append data
def appenddata(datelist,df):
    dflist = [df]
    for i in datelist:
        filename = str(i)+".export.csv"
        processed_data = processdata(filename)
        dflist.append(processed_data)
        print("%d concatenated" %i)
    new_df = pd.concat(dflist)
    print "Number of rows: %s" % (new_df.shape[0])
    return new_df
'''This way is slower
def appenddata(datelist,df):
    new_df = df
    for i in datelist:
        filename = str(i)+".export.csv"
        new_df = pd.concat([new_df,processdata(filename)])
        print("%d concatenated" %i)
    print "Total number of rows: %s" % (new_df.shape[0])
    return new_df
'''    
# Add column name and new 'ActionGeo_CountryName' column for new dataset
def processdata(filename):
    colsnames = pd.read_csv("header_fieldids_for_daily_data.csv")
    event = pd.read_csv(filename, sep="\t", names=colsnames["Field Name"].values.tolist())
    # Extract ActionGeo_CountryName from ActionGeo_FullName
    event["ActionGeo_CountryName"] = event.apply(lambda x: str(x["ActionGeo_FullName"]).split(',')[-1].strip(), axis=1)
    return event

old_monthlydata = pd.read_csv("monthlydata.csv")
# Data of certain date that already be included in new dataset
old_date = pd.unique(old_monthlydata.DATEADDED.ravel()).tolist()
yesterday = date.today() - timedelta(1)
# Data of all date that should be included in new dataset
all_date =[]
for i in range(0,29):
    all_date.append(date2num(yesterday-timedelta(i)))
# Data of certain date that should be kept of old dataset
keep_date = [x for x in old_date if x in all_date]
# Data of certain date that should be added to new dataset
new_date = [x for x in all_date if x not in old_date]

# Delete data that should not be included
kept_monthlydata = old_monthlydata[old_monthlydata['DATEADDED'].isin(keep_date)]
download(new_date)
# Add new data
new_monthlydata = appenddata(new_date,kept_monthlydata)

print ("data for" +str(pd.unique(new_monthlydata.DATEADDED.ravel()).tolist()))

# Save to csv
new_monthlydata.to_csv("monthlydata.csv", index=False)

print("Finished")