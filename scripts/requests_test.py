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

## Session setup
s = requests.Session()

retries = Retry(total=5,
                backoff_factor=0.1)

s.mount('http://', HTTPAdapter(max_retries=retries))

access_token = get_domain_access_token(session=s, client_creds_yml='client_credentials.yaml')

## Params
# access_token = access_token
# request_session = s
# postcode = ''
# state = 'Vic'
# region = ''
# area = ''
# listing_type = 'Sale'
# updated_since = ''#(dt.datetime.today() - dt.timedelta(days=10)).isoformat()
# listed_since = (dt.datetime.today() - dt.timedelta(days=10)).isoformat()

output = residential_listings_search(access_token = access_token,                        
                            request_session = s,
                            postcode = '',
                            state = 'Vic',
                            region = '',
                            area = '',
                            listing_type = 'Sale',
                            updated_since = '', #(dt.datetime.today() - dt.timedelta(days=10)).isoformat()
                            listed_since = (dt.datetime.today() - dt.timedelta(days=30)).isoformat()) 

# Now dealing with the nested data

listings = [value for value in output['listings'].values()]

listings_json = pd.json_normalize(listings)

projects = listings_json.loc[listings_json['type'] == 'Project']

property_listings = listings_json.loc[listings_json['type'] == 'PropertyListing']

if len(projects) > 0:
    ## Need to deal with projects that have nested json even after normalization - pd.json_normalize(test.loc[test['type'] == 'Project']['listings'][0])
    project_listings = pd.concat(list(map(pd.json_normalize, projects['listings']))).add_prefix('listing.')

    ## Getting all project data that are not dupes
    project_metadata = projects[projects.columns[~projects.columns.isin(project_listings.columns)]]
    
    project_metadata = project_metadata.drop_duplicates(subset='project.id')

    project_listings_w_meta = project_metadata.merge(project_listings, left_on='project.id', right_on='listing.projectId', how = 'left', validate = 'one_to_many')

    ## Dropping dupe col
    project_listings_w_meta = project_listings_w_meta.drop('listing.projectId', axis = 1)

    # Concatinating all listings together, dropping nested listings col (now joined on)
    all_listings = pd.concat([project_listings_w_meta, property_listings], ignore_index = True).drop('listings', axis = 1)

elif any(property_listings.columns == 'listings'):
    all_listings = property_listings.drop('listings', axis = 1)
else: 
    all_listings = property_listings

all_listings.columns = (all_listings.columns
                .str.replace('.', '_', regex=False)
             )

listed_since = output['listed_since_date']
max_listed_since_date = output['max_listed_since_date']
# example write
all_listings.to_csv(f'data/all_listings_vic.csv', index = False)
print(f'data/all_listings_vic_')

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

pd.read_sql('select * from download;', conn)

max_download_id = pd.read_sql('select max(id) as download_id from download;', conn)

## Striping object types of
all_listings_for_upload = all_listings.convert_dtypes().infer_objects()
all_listings_for_upload.update(all_listings_for_upload.select_dtypes('object').astype(str))
all_listings_for_upload['download_id'] = max_download_id['download_id'][0]

all_listings_for_upload.columns

conn.execute("VACUUM")
conn.commit()

cur.close()
# output
all_listings_for_upload.to_sql('raw_listing', conn, if_exists='append', index=False)

max_row = pd.read_sql('SELECT max(id) as max_id FROM raw_listing', conn)

out = pd.read_sql('select * from raw_listing;', conn)

out = pd.read_sql('PRAGMA table_info(listings_upload);', conn)

out.columns
out[['name', 'type']].to_clipboard(index=False)


out = pd.read_sql('select * from listings_upload', conn)

##push the dataframe to sql 
# my_data.to_sql("my_data", conn, if_exists="replace")

