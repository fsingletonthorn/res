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
from functions.database import get_last_download_metadata, update_listings_tables
from functions.helper_functions import clean_listings

## Session setup
s = requests.Session()

retries = Retry(total=5,
                backoff_factor=0.1)

s.mount('http://', HTTPAdapter(max_retries=retries))

# Grab domain access token
access_token = get_domain_access_token(session=s, client_creds_yml='client_credentials.yaml')

# Grab metadata from the last download
latest_metadata = get_last_download_metadata('res_database.db', listing_type = 'Sold')

## Grabbing the max listed date from the last download's metadata. This is used to 
if len(latest_metadata) > 0:
    listed_since_date = latest_metadata.max_listed_since_date[0]
else: 
    listed_since_date = (dt.datetime.today() - dt.timedelta(days=30)).isoformat()

output = residential_listings_search(access_token = access_token,                        
                            request_session = s,
                            postcode = '',
                            state =  '', ##'Vic',
                            region = '',
                            area = '',
                            listing_type = 'Sold',
                            updated_since = '', #(dt.datetime.today() - dt.timedelta(days=10)).isoformat()
                            listed_since =  listed_since_date)

all_listings = clean_listings(output)

all_listings.listing_soldData_soldPrice
 
if len(all_listings) > 0:
    update_listings_tables(output, all_listings)

print(f'{len(all_listings)} records added to raw listing tables, {output["pages_remaining"]} pages remaining after download completed.')

# Writing to csv
## all_listings.to_csv(f'data/all_listings_vic_{dt.date.today().isoformat()}.csv', index = False)


sold_since_last_check = residential_listings_search(access_token = access_token,                        
                            request_session = s,
                            postcode = '',
                            state =  '', ##'Vic',
                            region = '',
                            area = '',
                            listing_type = 'Sold',
                            updated_since = '', #(dt.datetime.today() - dt.timedelta(days=10)).isoformat()
                            listed_since = (dt.datetime.today() - dt.timedelta(days=10)).isoformat())

sold_since_last_check[['listing_2019459038']]

sold_since_last_check['listings']['listing_2019459038']