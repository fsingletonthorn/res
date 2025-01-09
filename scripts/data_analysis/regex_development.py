import pandas as pd
import datetime as dt
from functions.helper_functions import extract_price_info

sale_data = pd.read_csv('data/latest_sale_listings.csv',low_memory=False)

subset_for_testing=sale_data.sample(n=250, random_state=53812).filter(
                                        [   
                                            'listing_id',
                                            'listing_priceDetails_displayPrice',
                                            'listing_priceDetails_price',
                                            'listing_priceDetails_priceFrom',
                                            'listing_priceDetails_priceTo',
                                        ],
                                    axis = 1)

# subset_for_testing['estimated_price'] =
output = extract_price_info(subset_for_testing, 'listing_priceDetails_displayPrice')


output.to_csv(f'data/regex_test_cases_{dt.datetime.today().date().isoformat()}.csv')
