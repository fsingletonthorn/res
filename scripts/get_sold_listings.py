import requests
import datetime as dt
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

## Grabbing the last downloaded date from the last download's metadata. This is used to set when we download from. 
if len(latest_metadata) > 0:
    updated_since_date = latest_metadata.download_date[0]
else: 
    updated_since_date = (dt.datetime.today() - dt.timedelta(days=1)).isoformat()

output = residential_listings_search(access_token = access_token,                        
                            request_session = s,
                            postcode = '',
                            state =  '', ##'Vic',
                            region = '',
                            area = '',
                            listing_type = 'Sold',
                            updated_since = updated_since_date, #(dt.datetime.today() - dt.timedelta(days=10)).isoformat()
                            listed_since =  '')

all_listings = clean_listings(output)
 
if len(all_listings) > 0:
    update_listings_tables(output, all_listings)

print(f'{len(all_listings)} records added to raw sales listing table, {output["pages_remaining"]} pages remaining after download completed.')

# Writing to csv
## all_listings.to_csv(f'data/all_sold_listings_vic_{dt.date.today().isoformat()}.csv', index = False)