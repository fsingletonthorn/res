import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from collections.abc import MutableMapping
import time
from scraper.extract_features import extract_features

suburbs = 'coburg-vic-3058,brunswick-vic-3056'
page_num = 1
url = 'https://www.domain.com.au/sale/?suburb=' + suburbs + '&excludeunderoffer=1&sort=dateupdated-desc&page=' + str(page_num)
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'}
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, 'html.parser')

## Find list prices
listings = soup.findAll('li', attrs={"data-testid": re.compile("listing.*")})

## Helper functions
## Flatten dict generator
def _flatten_dict_gen(d, parent_key, sep):
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            yield from flatten_dict(v, new_key, sep=sep).items()
        else:
            yield new_key, v

## Flatten dict function
def flatten_dict(d: MutableMapping, parent_key: str = '', sep: str = '.'):
    return dict(_flatten_dict_gen(d, parent_key, sep))

# def extract_list_data(listing):
    
# listing_data = list()

for i in range(len(listings)): 
    print(i)
    listing = listings[i]
    test_hero = listing.find('div', attrs={"data-testid":'listing-card-project-hero'}) 
    if test_hero is not None:
        continue
    ## Direct URL
    listing_url = listing.find('a', href=True)['href']
    ## Extracting ID from URL
    id = re.search( '[0-9]*$', listing_url).group()
    # Price 
    price = listing.find('p', attrs={"data-testid": "listing-card-price"}).text
    # Address 
    address_line_1 = listing.find('span', attrs={"data-testid": "address-line1"}).text.split(',\xa0')[0]
    address_line_2 = listing.find('span', attrs={"data-testid": "address-line2"}).text
    # listing card extract - grabbing all features displayed
    #featuresWrapper = listing.find('div', attrs={"data-testid": "property-features"})
    
    features = extract_features(listing)

    features = listing.findAll('span', attrs={"data-testid": "property-features-text-container"})
    feature_dict = dict()
    # Bedrooms
    for j in range(len(features)):
       feature = features[j].find('span', attrs={"data-testid": "property-features-text"})
       if feature is None:
           feature = 'unknown'
       else:
           feature = feature.text
       value =  features[j].text.split(' ')[0]
       feature_dict.update({feature: value})

    dwelling_type = listing.find('span', attrs={"class": "css-693528"}).text
    
    # Type of residence
    # listing url
    listing_page = requests.get(listing_url, headers=headers)
    listing_soup = BeautifulSoup(listing_page.text, 'html.parser')
    
    agents = listing_soup.findAll('a', attrs={"data-testid": 'listing-details__agent-enquiry-agent-profile-link'})
    
    ## Build separate table of these guys
    agent_list = list()
    for j in range(len(agents)):
       agent = agents[j].find('h3',  attrs={"class": 'css-159ex32'}).text
       agency = agents[j].find('p',  attrs={"class": 'css-1vslaj8'}).text
       agent_list.append({'agent': agent, 'agency': agency})

    listing_extract = {'id': id, 'agent_list': agent_list, 
            'data': {
            'id': [id],
            'url': [listing_url],
            'price': [price], 
            'address_line_1': [address_line_1], 
            'address_line_2': [address_line_2], 
            'dwelling_type': [dwelling_type]
    }}

    listing_extract['data'].update(flatten_dict(feature_dict))

    listing_data.append(listing_extract)
    
    time.sleep(1)

