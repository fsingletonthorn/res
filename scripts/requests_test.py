#import json
import requests
import datetime as dt
#import re, string, timeit
import time
#import yaml
import pandas as pd 
#import logging
import math
import sqlite3
from requests.adapters import HTTPAdapter, Retry
from functions.domain import get_domain_access_token,residential_listings_search
from functions.database import get_last_download_metadata
from functions.helper_functions import clean_listings

## Session setup
s = requests.Session()

retries = Retry(total=5,
                backoff_factor=0.1)

s.mount('http://', HTTPAdapter(max_retries=retries))

# Grab domain access token
access_token = get_domain_access_token(session=s, client_creds_yml='client_credentials.yaml')

# Grab metadata from the last download
latest_metadata = get_last_download_metadata('res_database.db')

## Grabbing the max listed date from the last download's metadata. This is used to 
if len(latest_metadata) > 0:
    listed_since_date = latest_metadata.max_listed_since_date[0]
else: 
    listed_since_date = (dt.datetime.today() - dt.timedelta(days=1)).isoformat()

output = residential_listings_search(access_token = access_token,                        
                            request_session = s,
                            postcode = '',
                            state =  '', ##'Vic',
                            region = '',
                            area = '',
                            listing_type = 'Sale',
                            updated_since = '', #(dt.datetime.today() - dt.timedelta(days=10)).isoformat()
                            listed_since =  listed_since_date)


all_listings = clean_listings(output)

listed_since = output['listed_since_date']
max_listed_since_date = output['max_listed_since_date']

# example write
all_listings.to_csv(f'data/all_listings_vic_{dt.date.today().isoformat()}.csv', index = False)

conn = sqlite3.connect("res_database.db")
cur = conn.cursor()

download_meta = pd.DataFrame({
    'download_date': [dt.datetime.today().isoformat()], 
    'listed_since_date': [output['listed_since_date']],
    'max_listed_since_date': [output['max_listed_since_date']],
    'postcode': [output['postcode']],
    'state': [output['state']],
    'region': [output['region']],
    'area': [output['area']],
    'listing_type': [output['listing_type']],
    'updated_since': [output['updated_since']],
    'pages_remaining': [output['pages_remaining']]
    })

download_meta.to_sql('download', conn, if_exists='append', index=False)

# pd.read_sql('select * from download;', conn)

max_download_id = pd.read_sql('select max(id) as download_id from download;', conn)

## Striping object types of
all_listings_for_upload = all_listings.convert_dtypes().infer_objects()
all_listings_for_upload.update(all_listings_for_upload.select_dtypes('object').astype(str))
all_listings_for_upload['download_id'] = max_download_id['download_id'][0]

all_listings_for_upload.to_sql('raw_listing', conn, if_exists='append', index=False)

conn.execute("VACUUM")
conn.commit()

cur.close()
