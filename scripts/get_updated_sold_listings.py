import requests
import datetime as dt
from requests.adapters import HTTPAdapter, Retry
from functions.domain import get_domain_access_token,residential_listings_search
from functions.database import get_last_download_metadata, get_metadata, update_listings_tables
from functions.helper_functions import clean_listings
import pytz
import os

## Session setup
s = requests.Session()

retries = Retry(total=5,
                backoff_factor=0.1)

s.mount('http://', HTTPAdapter(max_retries=retries))

# Grab domain access token
access_token = get_domain_access_token(session=s, client_id= os.environ["CLIENT_ID"], client_secret=os.environ["CLIENT_SECRET"])

# Grab metadata from the last download
latest_metadata = get_last_download_metadata(listing_type = 'Sold Update', md_token=os.environ["MOTHERDUCK_TOKEN"])

## Grabbing the max listed date from the last download's metadata. This is used to filter down to not-downloaded records.
if len(latest_metadata) > 0 and latest_metadata.pages_remaining[0] ==  '0':
    updated_since_date = latest_metadata.download_date[0]
else:
    # failing that, just go back one day
    tz = pytz.timezone('Australia/Sydney')
    sydney_now = dt.datetime.now(tz)
    updated_since_date = (sydney_now - dt.timedelta(days=1)).isoformat()

output = residential_listings_search(access_token = access_token,                        
                            request_session = s,
                            postcode = '',
                            state =  '',
                            region = '',
                            area = '',
                            listing_type = 'Sold',
                            updated_since = updated_since_date,
                            listed_since =  '',
                            debug = False)

all_listings = clean_listings(output,
                            debug = False)

# Updating metadata to ID this as an update selection
output['listing_type'] = 'Sold Update'

if len(all_listings) > 0:
    update_listings_tables(raw_output= output,cleaned_listings= all_listings)

print(f'{len(all_listings)} records added to raw listing tables, {output["pages_remaining"]} pages remaining after download completed.')