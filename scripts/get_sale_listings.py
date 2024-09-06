import requests
import datetime as dt
from requests.adapters import HTTPAdapter, Retry
from functions.domain import get_domain_access_token,residential_listings_search
from functions.database import get_last_download_metadata, update_listings_tables
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
latest_metadata = get_last_download_metadata('res_database.db', listing_type = 'Sale')

## Grabbing the max listed date from the last download's metadata. This is used to filter down to not-downloaded records.
if len(latest_metadata) > 0:
    listed_since_date = latest_metadata.max_listed_since_date[0]
else:
    tz = pytz.timezone('Australia/Sydney')
    sydney_now = dt.datetime.now(tz)
    listed_since_date = (sydney_now - dt.timedelta(hours=1)).isoformat()

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


print(f"Checking database file: {'res_database'}")
print(f"Checking database file: {'res_database'}")
print(f"Exists: {os.path.exists('res_database.db')}")
print(f"Size: {os.path.getsize('res_database.db') if os.path.exists(db_path) else 'File does not exist'}")

if len(all_listings) > 0:
    update_listings_tables(raw_output= output,cleaned_listings= all_listings)

print(f'{len(all_listings)} records added to raw listing tables, {output["pages_remaining"]} pages remaining after download completed.')

# Writing to csv
## all_listings.to_csv(f'data/all_listings_vic_{dt.date.today().isoformat()}.csv', index = False)