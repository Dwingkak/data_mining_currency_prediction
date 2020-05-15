# -*- coding: utf-8 -*-
"""
Created on Fri May  8 15:59:37 2020

@author: Kdwing
"""

import findspark
findspark.init()

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()

#%% create new spark database
spark.sql("create database Project")

#%% create table for myr csv file
spark.sql("use Project")
spark.sql("create table malaysia_USD \
          (date DATE, USD_rate FLOAT) \
          row format delimited fields terminated by ','\
              stored as textfile")

#%% load data
spark.sql("load data local inpath './milestone_1/other/myr.csv'\
                 overwrite into table malaysia_USD")

#%% create table for top150 csv file

spark.sql("CREATE TABLE top150 " + 
          "(`Unnamed: 0` INT, name_trans STRING, " +
          "`viewData.symbol` STRING,sector_trans STRING, " + 
          "industry_trans STRING,last FLOAT, " +
          "pair_change_percent STRING,eq_market_cap STRING, " + 
          "turnover_volume STRING) " +
          "row format delimited fields terminated by ',' " + 
          "stored as textfile")
          
#%% load data
spark.sql("load data local inpath './milestone_1/other/top150.csv'\
                 overwrite into table top150")       
          
#%% create tables for historical data for   
import glob
# obtain all csv file path from directory         
csv_paths = glob.glob("./milestone_1/output/*.csv")

#%% putting files into hive
counter = 1
for csv_path in csv_paths:
    if counter % 10 == 0:
        print("[INFO] processing {}/{}".format(counter, len(csv_paths)))
    csv_file = csv_path.split("\\")
    name = csv_file[1][: csv_file[1].find(".")]
    path = csv_path.replace("\\", "/")
    spark.sql("CREATE Table " + name +
              " (`Unnamed: 0` INT, Date STRING, Price FLOAT, " +
              "Open FLOAT, High FLOAT, Low FLOAT, " +
              "`Vol.` STRING, `Change %` STRING) " +
              "row format delimited fields terminated by ',' " +
              "stored as textfile")
    spark.sql("load data local inpath '" + path +
              "' overwrite into table " + name)
    counter += 1
    









































