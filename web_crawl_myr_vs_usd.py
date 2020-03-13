# -*- coding: utf-8 -*-
"""
Created on Fri Mar 13 13:10:55 2020

@author: Kdwing
"""
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import Select
# read html from BNM website
url = ('https://www.bnm.gov.my/index.php?' + 
       'ch=statistic&pg=stats_exchangerates')
browser = webdriver.Chrome()
browser.get(url)

# select starting day and year from drop down menu
ddelement= Select(browser.find_element_by_xpath("/html/body/div[1]/div/div[2]/div/section/div/div/div/div/div[2]/div/form/div[1]/div[1]/select[1]"))
ddelement.select_by_index(0)
ddelement= Select(browser.find_element_by_xpath("/html/body/div[1]/div/div[2]/div/section/div/div/div/div/div[2]/div/form/div[1]/div[1]/select[2]"))
ddelement.select_by_value("2015")
browser.find_element_by_xpath("/html/body/div[1]/div/div[2]/div/section/div/div/div/div/div[2]/div/form/div[2]/div[4]/input").click()
time.sleep(2)

# extracting table from page
data = pd.read_html(browser.page_source)[0]
browser.close()

# keep only the first three columns
data = data.iloc[:,0:2]

# search for item with the name THB100
thb_index = data["USD"] == "THB100"
thb = data.index[tbh_index].tolist()

# removing unrelevant value at USD column
clean_data = data.iloc[:tbh[0] - 1]

# convert the column type to numeric
clean_data.loc[:,'USD'] = pd.to_numeric(clean_data.loc[:,'USD'])

# save dataframe into csv file
clean_data.to_csv("./other/myr.csv")




























