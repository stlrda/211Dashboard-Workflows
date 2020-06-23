import sys, os
import json
import pandas as pd
import numpy as np
import re
import requests
import datetime

"""
Extra functions used to transform data from the web (url).
Main transformer functions
    Arugments:
        url <str>
    Return:
        df <pandas df>

Name of transformer functions in use:
    transform_unemployment_stats

"""

# global variables

MONTH_DICT = {
    'Jan' : 1,
    'Feb' : 2,
    'Mar' : 3,
    'Apr' : 4,
    'May' : 5,
    'Jun' : 6,
    'Jul' : 7,
    'Aug' : 8,
    'Sep' : 9, 
    'Oct' : 10,
    'Nov' : 11,
    'Dec' : 12
}

""" helper functions """ 

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)

def unemployment_month_parser(text):
    mo, yr = text.split('-')
    mo_num = MONTH_DICT[mo]
    yr_num = int('20'+yr.replace('(p)', ''))
    return last_day_of_month(datetime.date(yr_num, mo_num, 1))


""" main functions """

def transform_unemployment_stats(url):
    myfile = requests.get(url)
    content = myfile.content
    content = content.decode("utf-8")
    records = [line.strip() for line in content.split('\n')]

    data = []
    for record in records:
        try:
            cnty = record.split('|')[3].strip()
            if cnty.endswith('MO') or cnty.endswith('IL'):
                data.append([item.strip() for item in record.split('|')[1:]])
        except:
            continue
    
    headers = ['state_fips_cd', 'county_fips_cd', 'county', 'month_last_date', 
               'labor_force', 'employed', 'unemployed', 'unemp_rate']
    df = pd.DataFrame(data, columns=headers)
    df.insert(2, 'geo_id', df['state_fips_cd']+df['county_fips_cd'])
    df.pop('county')
    df['month_last_date'] = df['month_last_date'].map(lambda x: unemployment_month_parser(x))
    df['labor_force'] = df['labor_force'].str.replace(',', '')
    df['employed'] = df['employed'].str.replace(',', '')
    df['unemployed'] = df['unemployed'].str.replace(',', '')

    # convert datatypes for counts/rate
    return df.astype({
        'labor_force': int,
        'employed': int,
        'unemployed': int,
        'unemp_rate': float}
    )