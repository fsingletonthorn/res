#import json
import requests
import datetime as dt
#import re, string, timeit
import time
#import yaml
import pandas as pd 
#import logging
import math
#import sqlite3
from requests.adapters import HTTPAdapter, Retry
from functions.domain import residential_listings_search,get_domain_access_token

## Session setup
s = requests.Session()

retries = Retry(total=5,
                backoff_factor=0.1)

s.mount('http://', HTTPAdapter(max_retries=retries))

access_token = get_domain_access_token(session=s, client_creds_yml='client_credentials.yaml')

## Params
access_token = access_token
request_session = s
postcode = ''
state = 'Vic'
region = ''
area = ''
listing_type = 'Sale'
updated_since = ''#(dt.datetime.today() - dt.timedelta(days=10)).isoformat()
listed_since = (dt.datetime.today() - dt.timedelta(days=30)).isoformat()


def residential_listings_search_loop(access_token = None, 
                                    request_session = None,
                                    postcode = None,
                                    state = None,
                                    region = None,
                                    area = None,
                                    listing_type = None,
                                    updated_since = None,
                                    listed_since= None): 
    # Initialising variables
    updating = True
    pageNumber = 0
    download_date = dt.datetime.today().isoformat()

    while updating:
        pageNumber  += 1

        request = residential_listings_search(
                                    access_token = access_token, 
                                    request_session = s,
                                    page_number = pageNumber,
                                    postcode = postcode,
                                    state = state, 
                                    region = region,
                                    area = area, 
                                    listing_type = listing_type, 
                                    updated_since = updated_since,
                                    listed_since= listed_since
                                    )
        
        # Error out if status code not 200
        if request.status_code != 200:
            raise ValueError('Error: Request status code request was: ' + str(request.status_code) + ', not 200')

        # Counting number of pages required here
        if pageNumber == 1:
            
            # Calculations for number of loops
            page_size = pd.to_numeric(request.headers['X-Pagination-PageSize'])
            num_records = pd.to_numeric(request.headers['X-Total-Count'])
            total_pages = math.ceil(num_records / page_size)

            # Checking that we are not going beyond the pagination limit
        # if total_pages > 10:
        #     raise ValueError('Error: Request returns: ' + str(num_records) + ', total records, narrow search parameters to only include 1000 listings at most to include all listings')

            # Checking that we are not going beyond the pagination limit
            if num_records == 0:
                raise ValueError('Error: Request returns: ' + str(num_records) + ' records, check search parameters')
    
        # Getting Json responses
        request_json = request.json()  

        # Saving or extending json
        if pageNumber == 1:
            listings = request_json
        else:
            listings.extend(request_json)

        print('Page ' + str(pageNumber) + ' of ' + str(total_pages) + ' (' + str(len(listings)) + ' of ' + str(num_records) + ' total records downloaded)')
        # Check x-total count, if greater than 1000 narrow search params
        # Check x-total count and iterate through pages if required
        
        # sleep a bit so you don't make too many API calls too quickly  ~ this should prevent us from sending more than 10 requests in a second
        time.sleep(0.4)  
        updating = pageNumber < total_pages

        # Inserting downloaded date
        for i in range(len(listings)): 
            listings[i]['download_date'] = download_date

        if pageNumber == 10:
            break

                    # Checking that daily quotas have not been reached
        if pd.to_numeric(request.headers['X-Quota-PerDay-Remaining']) == 0:
            print('X-Quota-PerDay-Remaining = 0')
            break

    meta = {'updated_since': updated_since,
             'listed_since': listed_since, 
             'daily_quota_remaining': pd.to_numeric(request.headers['X-Quota-PerDay-Remaining']),
             'pages_remaining': total_pages - pageNumber
             }

    out = {'listings': listings, 'meta': meta}

    return(out)

listings_w_meta = residential_listings_search_loop(access_token = access_token, 
                                    request_session = s,
                                    postcode = postcode,
                                    state = state, 
                                    region = region,
                                    area = area, 
                                    listing_type = listing_type, 
                                    updated_since = updated_since,
                                    listed_since= listed_since)

listings = listings_w_meta['listings']

# Now dealing with the nested data
listings_json = pd.json_normalize(listings)

projects = listings_json.loc[listings_json['type'] == 'Project']

property_listings = listings_json.loc[listings_json['type'] == 'PropertyListing']

if len(projects) > 0:
    ## Need to deal with projects that have nested json even after normalization - pd.json_normalize(test.loc[test['type'] == 'Project']['listings'][0])
    project_listings = pd.concat(list(map(pd.json_normalize, projects['listings']))).add_prefix('listing.')

    ## Getting all project data that are not dupes
    project_metadata = projects[projects.columns[~projects.columns.isin(project_listings.columns)]]

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

max_listed = pd.to_datetime(listings_json['listing.dateListed']).max().isoformat()

previous_listed_since = listed_since

# example write
all_listings.to_csv(f'data/all_listings_vic_{previous_listed_since}-{max_listed}.csv')

listed_since = max_listed