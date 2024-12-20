import pandas as pd
import re
import datetime as dt
# from functions.helper_functions import extract_price_info

def extract_price_info(df, price_column):
    def process_price(price):
        if pd.isna(price):
            return pd.Series({'no_price_provided': True, 'point_estimate': None, 'lower_bound': None, 'upper_bound': None})
        
        price = str(price).replace(',', '').lower()
        
        # First check for phone numbers and return early if found
        phone_patterns = [
            r'\b04\d{2}\s?\d{3}\s?\d{3}\b',  # Mobile starting with 04
            r'\b\+614\d{2}\s?\d{3}\s?\d{3}\b',  # International mobile format
            r'\b1300\s?\d{3}\s?\d{3}\b',  # 1300 numbers
            r'\b1800\s?\d{3}\s?\d{3}\b'   # 1800 numbers
        ]
        
        # Check if the string contains a phone number
        if any(re.search(pattern, price) for pattern in phone_patterns):
            return pd.Series({'no_price_provided': True, 'point_estimate': None, 'lower_bound': None, 'upper_bound': None})
        
        def convert_to_full_number(num_str, shared_suffix=None):
            num_str = num_str.lower().replace(' ', '')
            if shared_suffix:
                num_str = num_str + shared_suffix
            if 'k' in num_str:
                return float(num_str.replace('k', '')) * 1000
            elif 'm' in num_str:
                return float(num_str.replace('m', '')) * 1000000
            else:
                return float(num_str)

        def validate_prices(*args):
            """Validate that all non-None price values are >= 10000"""
            return all(x is None or x >= 10000 for x in args)

        # Extract price information
        number_pattern = '(\d+(?:\.\d+)?(?:[,.\s]\d{3})*(?:k(?!m)|m(?!2))?)(?!\s?sqm|am|pm)'
        price_range_pattern = rf'\$?\s*{number_pattern}\s*(?:-|to)\s*\$?\s*{number_pattern}'
        single_price_pattern = rf'\$?\s*{number_pattern}'
        offers_above_pattern = rf'(?:from|over|above|starting|offers\+)\s*\$?\s*{number_pattern}'
        
        price_range_shared_suffix = rf'\s*\$?\s*(\d+(?:\.\d+)?)\s*(?:-|to)\s*(\d+(?:\.\d+)?)(k(?!m)|m(?!2))'
        # First check for price range with shared suffix
        shared_suffix_match = re.search(price_range_shared_suffix, price, re.IGNORECASE)
        if shared_suffix_match and not re.search(r'\d+(?::\d+|am|pm)', price, re.IGNORECASE):
            lower = convert_to_full_number(shared_suffix_match.group(1), shared_suffix_match.group(3))
            upper = convert_to_full_number(shared_suffix_match.group(2), shared_suffix_match.group(3))
            if validate_prices(lower, upper):
                return pd.Series({'no_price_provided': False, 'point_estimate': None, 'lower_bound': lower, 'upper_bound': upper})

        # Check for price range (e.g., $550,000 - $600,000)
        range_match = re.search(price_range_pattern, price, re.IGNORECASE)
        if range_match:
            lower = convert_to_full_number(range_match.group(1))
            upper = convert_to_full_number(range_match.group(2))
            if validate_prices(lower, upper):
                return pd.Series({'no_price_provided': False, 'point_estimate': None, 'lower_bound': lower, 'upper_bound': upper})

        # Check for "offers over" or similar patterns
        offer_match = re.search(offers_above_pattern, price, re.IGNORECASE)
        if offer_match:
            offer_price = convert_to_full_number(offer_match.group(1))
            if validate_prices(offer_price):
                return pd.Series({'no_price_provided': False, 'point_estimate': None, 'lower_bound': offer_price, 'upper_bound': None})
        
        # Check for single price (e.g., $1,150,000)
        single_match = re.search(single_price_pattern, price)
        if single_match:
            point_estimate = convert_to_full_number(single_match.group(1))
            if validate_prices(point_estimate):
                return pd.Series({'no_price_provided': False, 'point_estimate': point_estimate, 'lower_bound': None, 'upper_bound': None})
        
        # If no price information is found or validation failed
        return pd.Series({'no_price_provided': True, 'point_estimate': None, 'lower_bound': None, 'upper_bound': None})

    # Apply the function to the price column
    result = df[price_column].apply(process_price)
    
    # Combine the result with the original dataframe
    return pd.concat([df, result], axis=1)

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



### NEeed to deal with these:
## Potentially - drop anything under e.g., 50k, maybe see if we can extend 
# Incorrect values: 
# listing_id	listing_priceDetails_displayPrice	listing_priceDetails_price	listing_priceDetails_priceFrom	listing_priceDetails_priceTo	no_price_provided	point_estimate	lower_bound	upper_bound
# 95362	2019257200	Various Land Sizes from 301 sqm to 368 sqm				FALSE		301	
# 72038	2019520300	Low-Mid $600's				FALSE	600		
# 2588	2019449300	Price Guide $1.35-1.45m				FALSE		1.35	1450000
# 32652	2019482500	GRAND OPENING THIS SATURDAY 1.30-2PM				FALSE		1.3	2