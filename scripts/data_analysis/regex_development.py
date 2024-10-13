import pandas as pd
import re

def extract_price_info(df, price_column):
    def process_price(price):
        if pd.isna(price):
            return pd.Series({'no_price_provided': True, 'point_estimate': None, 'lower_bound': None, 'upper_bound': None})
        
        price = str(price).replace(',', '').lower()
        
        def convert_to_full_number(num_str):
            num_str = num_str.lower().replace(' ', '')
            if 'k' in num_str:
                return float(num_str.replace('k', '')) * 1000
            elif 'm' in num_str:
                return float(num_str.replace('m', '')) * 1000000
            else:
                return float(num_str)

        # Extract price information
        price_range_pattern = r'\$?\s*([\d.]+(?:k|m)?)\s*(?:-|to)\s*\$?\s*([\d.]+(?:k|m)?)'
        single_price_pattern = r'\$\s*([\d.]+(?:k|m)?)'
        offers_above_pattern = r'(?:from|over|above|starting|offers|guide)\s+\$\s*([\d.]+(?:k|m)?)'
        
        # Check for price range (e.g., $550,000 - $600,000)
        range_match = re.search(price_range_pattern, price, re.IGNORECASE)
        if range_match:
            lower = convert_to_full_number(range_match.group(1))
            upper = convert_to_full_number(range_match.group(2))
            return pd.Series({'no_price_provided': False, 'point_estimate': None, 'lower_bound': lower, 'upper_bound': upper})
        
        # Check for single price (e.g., $1,150,000)
        single_match = re.search(single_price_pattern, price)
        if single_match:
            point_estimate = convert_to_full_number(single_match.group(1))
            return pd.Series({'no_price_provided': False, 'point_estimate': point_estimate, 'lower_bound': None, 'upper_bound': None})
        
        # Check for "offers over" or similar patterns
        offer_match = re.search(offers_above_pattern, price, re.IGNORECASE)
        if offer_match:
            offer_price = convert_to_full_number(offer_match.group(1))
            return pd.Series({'no_price_provided': False, 'point_estimate': None, 'lower_bound': offer_price, 'upper_bound': None})
        
        # If no price information is found
        return pd.Series({'no_price_provided': True, 'point_estimate': None, 'lower_bound': None, 'upper_bound': None})

    # Apply the function to the price column
    result = df[price_column].apply(process_price)
    
    # Combine the result with the original dataframe
    return pd.concat([df, result], axis=1)
# Example usage:
# df = pd.read_csv('your_file.csv')
# df_with_extracted_prices = extract_price_info(df, 'listing_priceDetails_displayPrice')

sale_data = pd.read_csv('data/latest_sale_listings.csv')

subset_for_testing=sale_data.dropna(subset=['listing_priceDetails_displayPrice']
                                    ).sample(n=100).filter(
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
output.to_csv('test_cases_with_additional_data_4.csv')