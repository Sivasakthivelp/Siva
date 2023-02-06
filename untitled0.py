# -*- coding: utf-8 -*-
"""
Created on Tue Dec 13 16:52:42 2022

@author: Sivasakthivel
"""
from pyspark.sql import SparkSession
from datetime import datetime
import pytz
import logging
import logging_json
spark = SparkSession.builder.appName("logging").getOrCreate()
current_dt=datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%m%d_%H%M%S')
directory="C:///Logging_file_creation/"
logfilename="siva"+current_dt+".log"
print(logfilename)
finalpath=directory+logfilename

print(finalpath)


logger=logging.getLogger('demologging')
logger.setLevel(logging.DEBUG)
FileHandler=logging.FileHandler(finalpath,mode='a+z')
formatter = logging_json.JSONFormatter(fields={
    "time": "asctime",
    "level_name": "levelname",
    "message": "message"
},
datefmt='%m|%d|%Y%I:%M:%S%p')
FileHandler.setFormatter(formatter)
logger.addHandler(FileHandler)
logger.debug('debug message')
logger.info('info message')
logger.warning('Warn message')
logger.error('error message')
logger.critical('critical message')



try:
 lines = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///SparkCourse/friends-by-age.csv")
 print("The file uploated successfully")
except Exception as e:
  logger.error(e)