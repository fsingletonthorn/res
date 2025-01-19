import pandas as pd
import datetime as dt
from functions.helper_functions import extract_price_info


## Sold listings to local
sold_listings = pd.read_csv('data/latest_sold_listings.csv')

## Sale listings to local
sale_listings = pd.read_csv('data/latest_sale_listings.csv')
sale_listings.columns

matched_records = sold_listings.merge(sale_listings, how = 'inner', on='listing_listingSlug', suffixes=['_sold','_sale'])

## Filtering incorrect price details w/ dates that break the price extractor
matched_records = matched_records[~matched_records['listing_priceDetails_displayPrice_sale'].str.contains("([1-9] |1[0-9]| 2[0-9]|3[0-1])(.|-)([1-9] |1[0-2])(.|-|)20[0-9][0-9]$", regex= True)]

## Adding in price details
matched_records = extract_price_info(matched_records, 'listing_priceDetails_displayPrice_sale')

# matched_records['']
# matched_records.rename(columns={"no_price_provided": "no_displayPrice_provided_sale", "point_estimate": "point_estimate_displayPrice_provided_sale"})

# matched_records['sale_price'] = matched_records['listing_priceDetails_price_sale'].combine_first()