# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 19:54:49 2020

@author: Kdwing
"""

from selenium import webdriver
from bs4 import BeautifulSoup
import time

print("loading page...")
browser = webdriver.Chrome()
browser.get("https://www.investing.com/stock-screener/" +
                        "?sp=country::42|sector::a|industry::a|" +
                        "equityType::a|exchange::62%3Ceq_market_cap;1")
time.sleep(10)
#%% enable the sector and industry columns
# reattempt for 10 times if item cannot be interacted
def closePopUp(browser):
    try:
        print("closing popup menu...")
        close_popUp = browser.find_element_by_class_name("popupCloseIcon")
        close_popUp.click()
    except:
        print("retry in 1s...")
        time.sleep(1)

def enableScreener(browser):
        try:
            print("enabling screener menu...")
            enable_screener = browser.find_element_by_id("colSelectIcon_stock_screener")
            enable_screener.click()
        except:
            print("retry in 1s...")
            time.sleep(1)

enableScreener(browser)
time.sleep(1)
for i in range(10):
    try:
        print("enabling sector column...")
        enable_sector = browser.find_element_by_id("SS_4")
        enable_sector.click()
        break
    except:
        closePopUp(browser)
        enableScreener(browser)
        print("retry in 1s...")
        time.sleep(1)
time.sleep(1)
for i in range(10):
    try:
        print("enabling industry column...")
        enable_industry = browser.find_element_by_id("SS_5")
        enable_industry.click()
        break
    except:
        closePopUp(browser)
        enableScreener(browser)
        print("retry in 1s...")
        time.sleep(1)
time.sleep(1)
for i in range(10):
    try:
        print("applying changes...")
        apply_changes = browser.find_element_by_id("selectColumnsButton_stock_screener")
        apply_changes.click()
        break
    except:
        closePopUp(browser)
        enableScreener(browser)
        print("retry in 1s...")
        time.sleep(1)


#%% parse the page with beautiful soup to extract the table from the page
def extract_rows(browser):
    print("parsing page with BeautifulSoup...")
    soup = BeautifulSoup(browser.page_source, "html.parser")
    print("extracting table from the page...")
    get_table = soup.find_all(id = "resultsTable")
    get_table_body = get_table[0].find("tbody")
    get_rows = get_table_body.find_all("tr")
    return get_rows

get_rows = extract_rows(browser)
#%% get headers
print("extracting headers from the table...")
get_a_row = get_rows[0].find_all("td")
headers = [i.get("data-column-name") for i in get_a_row
           if i.get("data-column-name")]

#%% grab the data from the table in page 1
print("extracting data from table..")

def extract_data(get_rows, browser, headers):
    rows = []
    links = []
    for page in range(1, 4):
        if page != 1:
            for k in range(10):
                try:
                    browser.find_element_by_xpath('/html/body/div[5]/section' +
                                                  '/div[12]/div[5]/div[2]/' +
                                                  'div[2]/a[{}]'.format(page)).click()
                    time.sleep(10)
                    get_rows = extract_rows(browser)
                    break
                except:
                    closePopUp(browser)
                    print("retring to change page in 1s...")
                    time.sleep(1)
        links.extend(extractLink(get_rows))
        for row in get_rows:
            row_data = row.find_all("td")
            # to match with headers
            extracted_data = []
            for data in row_data:
                if data.get("data-column-name") in headers:
                    extracted_data.append(data.get_text())
            rows.append(extracted_data)    
    return rows, links

# extract link of each stock
def extractLink(get_rows):
    links = []
    base = "https://www.investing.com/"
    for row in get_rows:
        link = base + row.find("a").get("href")
        links.append(link)
    return links

rows, links = extract_data(get_rows, browser, headers)

#%% saving data with pandas
import pandas as pd
df = pd.DataFrame(rows, columns = headers)

#%% saving dataframe in csv format
df.to_csv("top150.csv")

#%% 

    









































