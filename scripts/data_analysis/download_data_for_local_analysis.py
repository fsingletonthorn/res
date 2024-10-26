import os
from functions.database import get_raw_sale_listings, get_raw_sold_listings

## Sold listings to local
sold_listings = get_raw_sold_listings(md_token = os.environ["MOTHERDUCK_TOKEN"])
sold_listings.to_csv('data/latest_sold_listings.csv')

## Sale listings to local
sale_listings = get_raw_sale_listings(md_token = os.environ["MOTHERDUCK_TOKEN"])
sale_listings.to_csv('data/latest_sale_listings.csv')

