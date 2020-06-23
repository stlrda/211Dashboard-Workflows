import sys, os
import json
import pandas as pd
import numpy as np
import re
import requests
import datetime
import boto3
import io
import logging

"""
Extra functions used to transform data in project's s3 bucket.
Main transformer functions
    Arugments:
        data <str> : file or folder name
        resource_path <str> : path to data dictionary resources
        s3_conn <conn> : s3 connection object
        bucket <str> : s3 bucket name
    Returns:
        df <pandas df> : transformed dataframe

Name of transformer functions in use:
    transform_census_data
    transform_funding_data
    transform_crosswalk_files
    generate_areas_of_interest

"""

# global variables

STATE_DICT = {'MO': '29', 'IL': '17'}

""" helper functions """ 

#### Census Helper Functions:

def census_equation_reader(df, dval, level):
    ids = df.iloc[:,0]
    equation = dval['equation']
    if equation['operation'] == 'SUM':
        df = df.iloc[:,2:]
        calc_df = df[equation['columns']]
        calc_df.insert(0, equation['result'], calc_df.sum(axis=1, skipna=True))
    
    # add additional equation operations as needed
    final_cols = ['id']
    if 'columns' in dval:
        for col in reversed(dval['columns']):
            calc_df.insert(0, col, df[col])
        final_cols = final_cols + dval['columns']
    final_cols.append(equation['result'])
    
    if level == 'tract':
        calc_df.insert(0, 'id', ids.map(lambda x: re.search(r'^\d+US(\d{11})$', x).group(1)))
    else:
        calc_df.insert(0, 'id', ids.map(lambda x: re.search(r'^\d+US(\d{5})$', x).group(1)))
        
    df = calc_df[final_cols]
    if 'rename' in dval:
        df.rename(columns=dval['rename'], inplace=True)
    
    return df

def census_string_matcher(df, dval, level):
    ids = df.iloc[:,0]
    matcher = dval['stringmatch']
    if matcher['operation'] == 'startswith':
        df = df.iloc[:,2:]
        cols = [col for col in df.columns if col.startswith(matcher['string'])]
        if 'columns' in dval:
            cols = dval['columns'] + cols
        df = df[cols]
    
    # add additional string match operations as needed
    
    if 'rename' in dval:
        df.rename(columns=dval['rename'], inplace=True)
    
    if level == 'tract':
        df.insert(0, 'id', ids.map(lambda x: re.search(r'^\d+US(\d{11})$', x).group(1)))
    else:
        df.insert(0, 'id', ids.map(lambda x: re.search(r'^\d+US(\d{5})$', x).group(1)))
    
    return df
    
def census_mua_parser(path, dval, level, s3_conn, bucket_name):
    bucket = s3_conn.Bucket(bucket_name)
    file = [obj.key for obj in list(bucket.objects.filter(Prefix=f'{path}/'))].pop()
    obj = s3_conn.meta.client.get_object(Bucket=bucket_name, Key=file)
    mua = pd.read_csv(io.BytesIO(obj['Body'].read()), dtype={'MUA/P Area Code': object})
    mua = mua[dval['columns']]
    mua = mua[mua['Designation Type']=='Medically Underserved Area']
    mua = mua[mua['MUA/P Status Description']=='Designated']
    mua = mua[mua['MUA/P Area Code'].str.startswith((STATE_DICT['MO'], STATE_DICT['IL']))]
    if level == 'tract':
        mua = mua[mua['MUA/P Area Code'].str.len() == 11]
    else:
        mua = mua[mua['MUA/P Area Code'].str.len() == 5]
    mua['MUA/P Update Date String'] = mua['MUA/P Update Date String'].apply(pd.to_datetime)
    mua['MUA/P Designation Date String'] = mua['MUA/P Designation Date String'].apply(pd.to_datetime)
    mua.sort_values(by=['MUA/P Area Code','MUA/P Update Date String', 'MUA/P Designation Date String'], inplace=True, ascending=False)
    mua.drop_duplicates(subset='MUA/P Area Code', keep='first', inplace=True)
    # keep only "id" and "IMU Score" column
    mua.rename(columns={'MUA/P Area Code': 'id'}, inplace=True)
    mua=mua[['id', 'IMU Score']]
    return mua

def census_svi_parser(path, dval, level, s3_conn, bucket_name):
    bucket = s3_conn.Bucket(bucket_name)
    file = [obj.key for obj in list(bucket.objects.filter(Prefix=f'{path}/'))].pop()
    obj = s3_conn.meta.client.get_object(Bucket=bucket_name, Key=file)
    svi = pd.read_csv(io.BytesIO(obj['Body'].read()), dtype={'FIPS': object})
    svi = svi[dval['columns']]
    svi = svi[svi['FIPS'].str.startswith((STATE_DICT['MO'], STATE_DICT['IL']))]
    svi.replace(-999, np.nan, inplace=True)
    svi.rename(columns={'FIPS': 'id'}, inplace=True)
    return svi



#### Main Census Helper

def census_data_collector(data, data_dict, s3_conn, bucket_name):
    level = data.split('_').pop()
    logging.info(f'Collecting static census data from {bucket_name}')
    for key, val in data_dict.items():  # val represents metadata
        path = f'{data}/{key}'
        if key == 'mua':
            df = census_mua_parser(path, val, level, s3_conn, bucket_name)
            data_dict[key]['df'] = df
        elif key == 'svi':
            df = census_svi_parser(path, val, level, s3_conn, bucket_name)
            data_dict[key]['df'] = df
        else:
            bucket = s3_conn.Bucket(bucket_name)
            file = [obj.key for obj in list(bucket.objects.filter(Prefix=f'{path}/')) if 'data_with_overlays' in obj.key].pop()
            obj = s3_conn.meta.client.get_object(Bucket=bucket_name, Key=file)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()), header=1)
            if 'equation' in val:
                df = census_equation_reader(df, val, level)
                data_dict[key]['df'] = df
            elif 'stringmatch' in val:
                df = census_string_matcher(df, val, level)
                data_dict[key]['df'] = df
            else:
                ids = df.iloc[:,0]
                df = df.iloc[:,2:]
                df = df[val['columns']]
                if 'rename' in val:
                    df.rename(columns=val['rename'], inplace=True)
                if level == 'tract':
                    df.insert(0, 'id', ids.map(lambda x: re.search(r'^\d+US(\d{11})$', x).group(1)))
                else:
                    df.insert(0, 'id', ids.map(lambda x: re.search(r'^\d+US(\d{5})$', x).group(1)))
                data_dict[key]['df'] = df
    logging.info(f'Successfully transformed individual tables at the {level} level.')
    return data_dict


#### Final Census Helpers

def census_income_indicator(strval):
    strval = str(strval)
    if strval.endswith('+'):
        return '+'
    elif strval.endswith('-'):
        return '-'
    else:
        return np.nan


def census_median_income_formatter(df):
    str_start = 'Estimate!!Median household income in the past 12 months'
    new_col = 'Median income above or below indicator'

    for i, col in enumerate(df.columns):
        if col.startswith(str_start):
            df.insert(i+1, new_col, df[col].map(lambda x: census_income_indicator(x)))
            try:
                df[col] = df[col].str.rstrip('+-')
            except:
                pass
            
    return df


### Areas of Interest - helper functions
def flag_geo_areas(county, state, cnty_set):
    for cnty in cnty_set:
        if county==cnty['county'] and state==cnty['state']:
            return 'Y'
        else:
            continue
    
    return ''

def filter_geo_areas(df, geo_dict):
    idx = 3
    for area, cnty_set in geo_dict['areas'].items():
        df.insert(idx, f'{area}_flag', df.apply(lambda x: flag_geo_areas(county=x['county'], state=x['state'], cnty_set=cnty_set), axis=1))
        logging.info(f"Successfully flagged '{area}' area regions.")
        idx+=1
    
    state_tup = tuple(d['fips'] for d in geo_dict['states'])
    logging.info('Filtering for areas of interest...')
    fltrd_set = set(df[df['geo_id'].str.startswith(state_tup)].index)
    for i in range(3, len(df.columns)):
        fltrd_set.update(df[df.iloc[:,i]=='Y'].index)
    
    return df.filter(items=fltrd_set, axis=0)


""" main functions """

# census
def transform_census_data(data, resource_path, s3_conn, bucket_name):
    with open(f'{resource_path}census.json') as json_file:
        data_dict = json.load(json_file)
    
    data = census_data_collector(data, data_dict, s3_conn, bucket_name)

    left_key = list(data.keys())[0]
    logging.info("Joining census tables on 'id' field...")
    left = data[left_key]['df']
    right = None

    for key in list(data.keys())[1:]:
        right = data[key]['df']
        left = pd.merge(left,right, on=['id'], how='left', validate="one_to_one")

    # strip unnecessary punctuation
    left.replace(r'^-$', np.nan, regex=True, inplace=True)
    left.replace(r'^\*$', np.nan, regex=True, inplace=True)
    left.replace(',','', regex=True, inplace=True)

    # format median household income column
    df = census_median_income_formatter(left)
    return df

# funding
def transform_funding_data(data, resource_path, s3_conn, bucket_name):
    obj = s3_conn.meta.client.get_object(Bucket=bucket_name, Key=data)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), engine='python')
    df.replace('\n',' ', regex=True, inplace=True)
    df.replace('\r','', regex=True, inplace=True)
    return df

# HUD crosswalk
def transform_crosswalk_files(data, resource_path, s3_conn, bucket_name):
    path = 'crosswalk/'
    bucket = s3_conn.Bucket(bucket_name)
    file = [obj.key for obj in list(bucket.objects.filter(Prefix=f'{path}')) if data in obj.key].pop()

    data_type_dict = {w : object for w in data.split('_')}
    
    obj = s3_conn.meta.client.get_object(Bucket=bucket_name, Key=file)
    df = pd.read_excel(io.BytesIO(obj['Body'].read()), dtype=data_type_dict)
    fids = 'TRACT' if 'TRACT' in data_type_dict else 'COUNTY'

    return df[df[fids].str.startswith((STATE_DICT['MO'], STATE_DICT['IL']))]

# areas of interest
def generate_areas_of_interest(data, resource_path, s3_conn, bucket_name):  # data = 'census_county'
    """ 
    generates geoid, county_name table from census county data
    also uses areas_of_interest.csv file found in resource directory
    to provide appropriate 'labels'
    """
    
    with open(f'{resource_path}census.json') as json_census:
        census_dict = json.load(json_census)
        
    with open(f'{resource_path}areas_of_interest.json') as json_geo:
        geo_dict = json.load(json_geo)
    
    key = list(census_dict.keys())[0]  # arbitrarily choose first key to create county list from
    path = f'{data}/{key}'
    
    bucket = s3_conn.Bucket(bucket_name)
    file = [obj.key for obj in list(bucket.objects.filter(Prefix=f'{path}/')) if 'data_with_overlays' in obj.key].pop()
    obj = s3_conn.meta.client.get_object(Bucket=bucket_name, Key=file)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), header=1)
    df = df.iloc[:,0:2]
    
    df['county'], df['state'] = df['Geographic Area Name'].str.split(', ', 1).str
    df.insert(2, 'geo_id', df['id'].map(lambda x: re.search(r'^\d+US(\d{5})$', x).group(1)))
    df['county'] = df['county'].str.replace(' County', '')
    df['county'] = df['county'].str.replace(' city', ' City')
    df = df.iloc[:,2:5]
    
    geo_df = filter_geo_areas(df, geo_dict)
    return geo_df