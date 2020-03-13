# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 19:54:49 2020

@author: Kdwing
"""

from selenium import webdriver
from bs4 import BeautifulSoup
import time
import pandas as pd

print("loading page...")
browser = webdriver.Chrome()
browser.get("https://www.investing.com/stock-screener/" +
                        "?sp=country::42|sector::a|industry::a|" +
                        "equityType::a|exchange::62%3Ceq_market_cap;1")
print("waiting for 10s...")
time.sleep(10)
# enable the sector and industry columns
# reattempt for 10 times if item cannot be interacted
def closePopUp():
    '''close pop up menu'''
    try:
        print("closing popup menu...")
        close_popUp = browser.find_element_by_xpath("/html/body/div[7]/div[2]/i")
        close_popUp.click()
    except:
        print("fail to close pop up menu...")

def simulateClick(elementId):
    '''Simulate mouse click on given ID'''
    for i in range(10):
        try:
            browser.find_element_by_id(elementId).click()
            break
        except:
            closePopUp()
            print("retry in 1s...")
            time.sleep(1)
# simulate mouse click to open the screener menu 
# to enable sector and industry column
# close pop up menu if it appeared at the screen 
print("enabling screener menu...")
simulateClick("colSelectIcon_stock_screener")
time.sleep(1)
print("enabling sector column...")
simulateClick("SS_4")
time.sleep(1)
print("enabling industry column...")
simulateClick("SS_5")
time.sleep(1)
print("applying changes...")
simulateClick("selectColumnsButton_stock_screener")
time.sleep(1)

# parse the page with beautiful soup to extract the table from the page
def extract_rows(browser, tableId):
    '''extract rows from a table given an Id'''
    print("parsing page with BeautifulSoup...")
    soup = BeautifulSoup(browser.page_source, "html.parser")
    print("extracting table from the page...")
    get_table = soup.find(id = tableId)
    get_table_body = get_table.find("tbody")
    get_rows = get_table_body.find_all("tr")
    return get_rows

get_rows = extract_rows(browser, "resultsTable")
# get headers
print("extracting headers from the table...")
get_a_row = get_rows[0].find_all("td")
headers = [i.get("data-column-name") for i in get_a_row
           if i.get("data-column-name")]

# grab the data from the table in page 1
print("extracting data from table..")

def extract_data(get_rows, browser, headers):
    rows = []
    links = []
    for page in range(1, 4):
        if page != 1:
            for k in range(10):
                try:
                    # change table page
                    browser.find_element_by_xpath('/html/body/div[5]/section' +
                                                  '/div[12]/div[5]/div[2]/' +
                                                  'div[2]/a[{}]'.format(page)).click()
                    time.sleep(10)
                    get_rows = extract_rows(browser, "resultsTable")
                    break
                except:
                    closePopUp(browser)
                    print("retring to change page in 1s...")
                    time.sleep(1)
        links.extend(extractLink(get_rows, "https://www.investing.com/"))
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
def extractLink(get_rows, baseUrl):
    links = []
    base = baseUrl
    for row in get_rows:
        link = base + row.find("a").get("href")
        links.append(link)
    return links

rows, links = extract_data(get_rows, browser, headers)
print("closing browser...")
browser.close()
# saving data with pandas
df = pd.DataFrame(rows, columns = headers)

# saving dataframe in csv format
df.to_csv("./output/top150.csv")

# combining links and stocks name
links_name = dict(zip(df.name_trans.tolist(), links))

# loop through the links and open a new webdriver (for all 150 stocks)

for name, link in links_name.items():
    print("loading page...")
    browser = webdriver.Chrome()
    browser.get(link + "-historical-data")
    time.sleep(10)
    # simulate click on date widget
    print("Opening field date widget...")
    simulateClick("widgetFieldDateRange") 
    time.sleep(1)
    print("inputing star date...")
    sDate = browser.find_element_by_id("startDate")
    sDate.clear()
    sDate.send_keys("03/01/2015")
    simulateClick("applyBtn")    
    time.sleep(5)
    print("extracting data from table...")
    dfs = pd.read_html(browser.page_source, attrs = {"id" : "curr_table"})[0]
    browser.close()
    dfs['Date']= pd.to_datetime(dfs['Date'])
    print("saving dataframe to csv format...")
    dfs.to_csv("./output/" + name.replace(" ", "_") + ".csv")










































