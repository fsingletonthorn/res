import os
import pandas as pd
import duckdb
from functions.helper_functions import extract_price_info

sale_data = pd.read_csv('data/latest_sale_listings.csv')

subset_for_testing=sale_data.dropna(subset=['listing_priceDetails_displayPrice']
                                    ).sample(n=500).filter(
                                        [   
                                            'listing_id',
                                            'listing_priceDetails_displayPrice',
                                            'listing_priceDetails_price',
                                            'listing_priceDetails_priceFrom',
                                            'listing_priceDetails_priceTo',
                                        ],
                                    axis = 1)

subset_for_testing['listing_price_filled'] = ~subset_for_testing['listing_priceDetails_price'].isna()

subset_for_testing.to_csv('data/testing_subset.csv')
