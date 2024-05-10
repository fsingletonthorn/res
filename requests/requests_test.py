import json
import requests
import re, string, timeit
import time
import pandas as pd 
import logging
import math
from requests.adapters import HTTPAdapter, Retry


s = requests.Session()

retries = Retry(total=5,
                backoff_factor=0.1)

s.mount('http://', HTTPAdapter(max_retries=retries))

# Grabbing access token
response = s.post('https://auth.domain.com.au/v1/connect/token', data = {'client_id':'client_id',"client_secret":"client_secret","grant_type":"client_credentials","scope":"api_listings_read","Content-Type":"text/json"})
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
        page_size = pd.to_numeric(request.headers['X-Pagination-PageSize'])
        num_records = pd.to_numeric(request.headers['X-Total-Count'])
        total_pages = math.ceil(num_records / page_size)

    
    # Check x-total count, if greater than 1000 narrow search params
    # Check x-total count and iterate through pages if required


    l=request.json()
    
    for listing in l:
        listings.append(listing["listing"]["id"])
    listings

    
    if int(property_id) in listings:
            max_price=max_price-increment
            print("Lower bound found: ", max_price)
            updating=False
    else:
        max_price=max_price+increment
        print("Not found. Increasing max price to ",max_price)
        time.sleep(0.1)  # sleep a bit so you don't make too many API calls too quickly  

request = requests.get(url,headers=auth)
r=request.json()
test = pd.json_normalize(l)
test.columns
out = requests.post(url = 'https://api.domain.com.au/v1/listings/residential/_search',
    headers = {'accept': 'application/json',
               'Content-Type': 'application/json',
               'X-Api-Key': 'key_0b9947c57edbcc7f30770e7a3cea6908'
               },
    data= {'locations': [{"state: VIC","postCode: 3058"}]}
    )
print(out.request.headers)
