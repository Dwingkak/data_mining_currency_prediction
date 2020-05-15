# -*- coding: utf-8 -*-
"""
Created on Fri May  8 18:41:03 2020

@author: Kdwing
"""
#%% initiating pyspark
import findspark
findspark.init()

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sql("use Project")
#%% read csv from Hive (myr.csv)

myr_csv = spark.read.option("header", "true")\
            .option("inferSchema", "true")\
                .csv("./spark-warehouse/project.db/malaysia_usd")
myr_df = myr_csv.toPandas()

#%% Workflow of data cleaning

# in the database, it consists of three type of different
# table structure.
# 1) Exchange rate of MYR for the past five years
# 2) top 150 capital values companies
# 3) stock prices of the top 150 for the past five years

#%% data cleaning on Exchange rate of MYR
import pandas as pd
myr_df.info()

# data structure in the table:
# date: int64
# USD: float64

# check if any missing data in the dataframe
print("\n", myr_df.isnull().values.any())

# No missing data is found in the dataset. Therefore, no cleaning is required 
# for the dataset.

#%% loading Hive data for top150.csv

top150_csv = spark.read.option("header", "true")\
            .option("inferSchema", "true")\
                .csv("./spark-warehouse/project.db/top150")
top150_df = top150_csv.toPandas()

#%% data cleaning on dataframe with top 150 capital values
# remove extra index column
# drop unnessary columns: last, pair_change_percent, turnover
drop_columns = ["Unnamed: 0", "last", "pair_change_percent",
                "turnover_cap"]
top150_df.drop(labels = drop_columns, axis = 1, inplace = True)
# rename columns
top150_df.columns = ["name", "symbol", "sector", "industry", "market_cap"]

# replace NaN row with "unknown"
top150_df.sector[top150_df.sector.isnull()] = "unknown"
top150_df.industry[top150_df.industry.isnull()] = "unknown"

# create a new column and convert market capital from string to value
def convert_string_to_value(string):
    number = float(string[:-1])
    return number * 1e9

top150_df["market_cap_value"] = top150_df.market_cap.apply(convert_string_to_value)

top150_df.to_csv("./milestone_1/cleaned_data/top150/top150.csv", index = False)

#%% Create new database to store cleaned data

spark.sql("create database cleaned_data")
spark.sql("use cleaned_data")
#%% store top150 cleaned csv into Hive
spark.sql("CREATE TABLE top150 " + 
          "(name STRING, symbol STRING, " +
          "sector STRING, industry STRING, " + 
          "market_cap STRING, market_cap_value FLOAT) " +
          "row format delimited fields terminated by ',' " + 
          "stored as textfile")

#%% load table top150 into the database

spark.sql("load data local inpath './milestone_1/cleaned_data/top150/top150.csv'\
                 overwrite into table top150")

#%% obtain all path csv path names for stock price csv
import os

paths = os.listdir("./spark-warehouse/project.db")
unwanted_path = ["malaysia_usd", "top150"]
paths_150 = [path for path in paths if path not in unwanted_path]

#%% data clearning on stock prices for all 150 companies
import os

# a function to convert string column to float column
def conversion_string2value(string):
    last_char = string[-1]
    if last_char == "K":
        return float(string[:-1]) * 1e3
    elif last_char == "M":
        return float(string[:-1]) * 1e6

# obtain full list of csv for all 150 companies stock
full_list = os.listdir("./milestone_1/output")
# loop through the entire list and perform data cleaning
i = 1 # a counter
for path in full_list:
    if i % 10 == 0:
        print("[INFO] processing {}/{}".format(i, len(full_list)))
    df_company = pd.read_csv("./milestone_1/output/" + path)
    # remove index name, and change %
    drop_columns = ["Unnamed: 0", "Change %"]
    df_company.drop(labels = drop_columns, axis = 1, inplace = True)    
    # convert volume from string to float
    df_company["Vol."] = df_company["Vol."].apply(conversion_string2value)
    # save to file
    df_company.to_csv("./milestone_1/cleaned_data/companies/" + path, 
                      index = False)
    i += 1
    



































