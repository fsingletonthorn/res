import json
import requests
import re, string, timeit
import time
import yaml
import pandas as pd 
import logging
import math
from requests.adapters import HTTPAdapter, Retry

s = requests.Session()

retries = Retry(total=5,
                backoff_factor=0.1)

s.mount('http://', HTTPAdapter(max_retries=retries))

# Grabbing access token
with open('client_credentials.yaml', 'r') as f:
    doc = yaml.load(f, Loader=yaml.Loader)

response = s.post('https://auth.domain.com.au/v1/connect/token', 
                  data = {'client_id':doc[0]['client_id'],
                          "client_secret":doc[0]['client_secret'],
                          "grant_type":"client_credentials",
                          "scope":"api_listings_read",
                          "Content-Type":"text/json"}
                )

token=response.json()
access_token=token["access_token"]

## Example
# property_id="2014925785"
# # Getting a single property here
# url = "https://api.domain.com.au/v1/listings/"+property_id
# auth = {"Authorization":"Bearer "+access_token}
# request = s.get(url,headers=auth)
# r=request.json()

# Looping through available records
listings = []
state = 'VIC'
auth = {"Authorization":"Bearer "+access_token}
postCode = ''
updatedSince = "2024-05-10T00:00:00.000Z"
pageNumber = 0
url = "https://api.domain.com.au/v1/listings/residential/_search" # Set destination URL here
updating = True

while updating:

    pageNumber  += 1

    #headers = auth.update(additional_headers)
    post_fields ={
      'pageSize': 100,
      'pageNumber': pageNumber,
      "listingType":"Sale",
      "updatedSince":  updatedSince,
      "locations":[
        {
          "state":state,
          "region":"",
          "area":"",
          "postCode":postCode,
          "includeSurroundingSuburbs":False
        }
      ]
    }

    request = requests.post(url,headers=auth,json=post_fields)

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
        if total_pages > 100:
            raise ValueError('Error: Request returns: ' + str(num_records) + ', total records, narrow search parameters to include all listings')

                # Checking that we are not going beyond the pagination limit
        if num_records == 0:
            raise ValueError('Error: Request returns: ' + str(num_records) + ', check search parameters')

        if pd.to_numeric(request.headers['X-Quota-PerDay-Remaining']) < 1:
            raise ValueError('Error: ' + request.headers['X-Quota-PerDay-Remaining'] + ' quota per day remaining, no queries left')         

        # Initializing data store
        listings = request.json()

    else:
        listings.extend(request.json())

    print('Page ' + str(pageNumber) + ' of ' + str(total_pages) + ' (' + str(len(listings)) + ' of ' + str(num_records) + ' total records downloaded)')
    # Check x-total count, if greater than 1000 narrow search params
    # Check x-total count and iterate through pages if required
    
    # sleep a bit so you don't make too many API calls too quickly  ~ this should prevent us from sending more than 10 requests in a second
    time.sleep(0.1)  
    updating = pageNumber < total_pages


# Now dealing with the nested data
listings_json = pd.json_normalize(listings)

projects = listings_json.loc[listings_json['type'] == 'Project']

property_listings = listings_json.loc[listings_json['type'] == 'PropertyListing']

## Need to deal with projects that have nested json even after normalization - pd.json_normalize(test.loc[test['type'] == 'Project']['listings'][0])
project_listings = pd.concat(list(map(pd.json_normalize, projects['listings']))).add_prefix('listing.')

## Getting all project data that are not dupes
project_metadata = projects[projects.columns[~projects.columns.isin(project_listings.columns)]]

project_listings_w_meta = project_metadata.merge(project_listings, left_on='project.id', right_on='listing.projectId', how = 'left', validate = 'one_to_many')

## Dropping dupe col
project_listings_w_meta.drop('listing.projectId', axis = 1)

# Concatinating all listings together, dropping nested listings col (now joined on)
all_listings = pd.concat([project_listings_w_meta, listings_json], ignore_index = True).drop('listings', axis = 1)
