# -*- coding: utf-8 -*-
"""
Created on Fri May  8 18:41:03 2020

@author: Kdwing
"""

import pandas as pd

# in the database, it consists of three type of different
# table structure.
# 1) Exchange rate of MYR for the past five years
# 2) top 150 capital values companies
# 3) stock prices of the top 150 for the past five years

#%% data cleaning on Exchange rate of MYR
df_myr = pd.read_csv("./milestone_1/other/myr.csv")
df_myr.info()

# data structure in the table:
# date: int64
# USD: float64

# check if any missing data in the dataframe
print("\n", df_myr.isnull().values.any())

#%% data cleaning on dataframe with top 150 capital values
df_top150 = pd.read_csv("./milestone_1/other/top150.csv")
# remove extra index column
# drop unnessary columns: last, pair_change_percent, turnover
drop_columns = ["Unnamed: 0", "last", "pair_change_percent",
                "turnover_volume"]
df_top150.drop(labels = drop_columns, axis = 1, inplace = True)
# rename columns
df_top150.columns = ["name", "symbol", "sector", "industry", "market_cap"]

# replace NaN row with "unknown"
df_top150.sector[df_top150.sector.isnull()] = "unknown"
df_top150.industry[df_top150.industry.isnull()] = "unknown"

# create a new column and convert market capital from string to value
def convert_string_to_value(string):
    number = float(string[:-1])
    return number * 1e9

df_top150["market_cap_value"] = df_top150.market_cap.apply(convert_string_to_value)

df_top150.to_csv("./milestone_1/cleaned_data/top150/top150.csv", index = False)

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
    



































