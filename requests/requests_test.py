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
postCode = '3058'
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
        # Initializing data store
        listings = request.json()
    
    else:
        listings.extend(request.json())

    print('Page ' + str(pageNumber) + ' of ' + str(total_pages) + ' (' + str(len(listings)) + ' of ' + str(num_records) + ' total records downloaded)')
    # Check x-total count, if greater than 1000 narrow search params
    # Check x-total count and iterate through pages if required
    
    time.sleep(0.15)  # sleep a bit so you don't make too many API calls too quickly  ~ this should prevent us from using more than 10 in a second
    updating = pageNumber < total_pages


request = requests.get(url,headers=auth)
r=request.json()
test = pd.json_normalize(listings)
test.columns
out = requests.post(url = 'https://api.domain.com.au/v1/listings/residential/_search',
    headers = {'accept': 'application/json',
               'Content-Type': 'application/json',
               'X-Api-Key': 'key_0b9947c57edbcc7f30770e7a3cea6908'
               },
    data= {'locations': [{"state: VIC","postCode: 3058"}]}
    )
print(out.request.headers)
