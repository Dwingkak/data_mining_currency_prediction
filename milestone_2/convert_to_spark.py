# -*- coding: utf-8 -*-
"""
Created on Sun Mar 22 20:08:39 2020

@author: Kdwing
"""

#%% initiating spark in python
import findspark
findspark.init()

import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
print("initiating pyspark...")
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#%% read csv file as pandas dataframe and convert it into spark dataframe
import pandas as pd
import os

directory = input("Please insert the directory to load csv files...\n")
csv_files = []
root_dir = None
print("preparing csv file directories...")
for root, dirs, files in os.walk(directory):
    root_dir = root
    csv_files.extend(files)

output = input("Please insert the output directory...\n")
print("converting to spark dataframe...")
for file in csv_files:
    target = os.path.join(root_dir, file)
    print("converting {}...".format(file))
    df_pandas = pd.read_csv(target)
    try:
        df = spark.createDataFrame(df_pandas)
    except:
        print("attempting correction method...")
        columns = df_pandas.columns.to_list()
        object_list = [column for column in columns if df_pandas[column].dtype == 'O']
        df_pandas[object_list] = df_pandas[object_list].astype(str)
        df = spark.createDataFrame(df_pandas)
    output_dir = os.path.join(output, file)
    df.write.format("csv")\
        .mode("overwrite")\
            .option("header", "true")\
                .save(output_dir)



























