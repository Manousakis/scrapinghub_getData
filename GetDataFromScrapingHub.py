#!/usr/bin/python
# coding=utf-8
from scrapinghub import ScrapinghubClient
import unicodecsv as csv
import os
import logging
import pandas as pd
import datetime
import pickle

# Create and configure logger
LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
logging.basicConfig(level = logging.INFO,
                    format = LOG_FORMAT,
                    filemode = 'w')
logger = logging.getLogger()

logger.info("Starting downloading")

# Enter ScrapingHub
apikey = '9ba55f964e5f42e4a66b682f48c680fe'  # your API key as a string
client = ScrapinghubClient(apikey)
projectID = 236717
project = client.get_project(projectID)
#   # Give me a list of dictionaries with info (each for every spider i have)
spider_dicts_list = project.spiders.list()
for spider_dict in spider_dicts_list:
    #   # Extract from the list the id of my spider
    spiderID = spider_dict["id"]
    logger.info("Working with spider: " + spiderID)
    # Get that spider and assign it to the object "spider"
    spider = project.spiders.get(spiderID)
    # Get a generator object for the jobs of that spider
    jobs_summary = spider.jobs.iter()
    # Generate all job keys using the generator object
    job_keys = [j['key'] for j in jobs_summary]
    for job_key in job_keys:
        # Get the corresponding job from the key, as "job"
        job = project.jobs.get(job_key)
        # Check to see if the job was completed
        if job.metadata.get(u'close_reason') == u'finished':
            # Create an empty list that will store all items (dictionaries)
            itemsDataFrame = pd.DataFrame()
            for item_aggelia in job.items.iter():
                # Save all items (dictionaries) to the DataFrame
                itemsDataFrame = itemsDataFrame.append(item_aggelia, ignore_index=True)
                job_key_name = job_key.split("/")[2]
                # Export a pickle
                # Check that the list is not empty
            if not itemsDataFrame.empty:
                for meta in job.metadata.iter():
                    if meta[0] == u"scrapystats":
                        timestamp = meta[1][u'finish_time']/1000.0
                dt = datetime.datetime.fromtimestamp(timestamp)
                filename = spiderID+" "+str(dt.year)+"-"+str(dt.month)+"-"+str(dt.day)+" "+str(dt.hour)+"_"+str(dt.minute)+"_"+str(dt.second)+" "+'Items.pickle'
                directory = u"E:/Documents/OneDrive/4_Προγραμματισμός/Scrapy/Αγορά Ακινήτων/"+spiderID+u"/Αρχεία_pd.DataFrame"
                os.chdir(directory)
                with open(filename, 'w') as file:
                    pickle.dump(itemsDataFrame,file)
            # Check for empty fields
            colList = itemsDataFrame.columns.tolist()
            for col in colList:
                if itemsDataFrame[col].isnull().all():
                    logger.warning("Found Null Field, in job " + job_key_name +": " + col)
            # Delete the job from ScrapingHub
            logger.debug("Deleting job " + job_key_name)
            job.delete()
        else:
            logger.info("Found a job that didn't finish properly. Job key: " + job_key+". close_reason:" + job.metadata.get(u'close_reason'))
