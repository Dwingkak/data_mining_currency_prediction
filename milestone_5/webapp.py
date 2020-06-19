# -*- coding: utf-8 -*-
"""
Created on Fri Jun 19 14:45:07 2020

@author: Kdwing
"""

import streamlit as st
import pandas as pd
import numpy as np
import datetime
import pickle
import xgboost as xgb

st.title("MYR Exchange Rate Estimation (10 days)")

@st.cache
def load_data(path, checkDate = True, sortValue = True):
    if checkDate:
        df = pd.read_csv(path, parse_dates = ["date"])
    else:
        df = pd.read_csv(path)
    if sortValue:
        df.sort_values("date", ascending = True, inplace = True)
    return df

PATH = "D:\\master_of_data_science\\Data_Mining\\data_mining_currency_prediction\\milestone_5\\data\\myr.csv"
data_load_state = st.text("Loading data...")
data = load_data(PATH)
data_load_state.text("Loading data...Done!")

if st.checkbox("Show raw data"):
    st.subheader("Raw data")
    st.write(data.set_index("date"))

st.subheader("Malaysian Exchange Rate in USD")
start_date = st.sidebar.date_input("start date", datetime.date(2015, 1, 6))
end_date = st.sidebar.date_input("end date", datetime.date(2020, 5, 1))
new_data = data[(data["date"].dt.date >= start_date) &
                (data["date"].dt.date <= end_date)]
st.line_chart(new_data.set_index("date"))

if st.checkbox("Start estimation"):
    st.subheader("Exchange Rate Estimation")
    
    test_path = "D:\\master_of_data_science\\Data_Mining\\data_mining_currency_prediction\\milestone_5\\data\\test_df.csv"
    example_path = "D:\\master_of_data_science\\Data_Mining\\data_mining_currency_prediction\\milestone_5\\data\\example.csv"
    
    model = xgb.XGBRegressor()
    model.load_model("xgboost.model")
    
    test_df = load_data(test_path, True, True)
    example_df = load_data(example_path, True, True)
    last_10days = test_df.set_index("date")[["exchange_rate"]].copy()
    
    last_10days["prediction"] = pd.Series(model.predict(test_df.set_index("date")), 
                                          index = test_df.set_index("date").index)
    final_df = pd.concat([last_10days, 
                          example_df.set_index("date")], 
                         sort = False).sort_index()
    cut_off = datetime.date(2019, 11, 1)
    st.line_chart(final_df[final_df.index.date > cut_off])




























