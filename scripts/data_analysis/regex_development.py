import pandas as pd
import re

def extract_price_info(df, price_column):
    def process_price(price):
        price = str(price)  # Convert to string to handle NaN values
        
        # Remove commas and convert to lowercase
        price = price.replace(',', '').lower()
        
        def convert_to_full_number(num_str):
            num_str = num_str.lower().replace(' ', '')
            if 'k' in num_str:
                return float(num_str.replace('k', '')) * 1000
            elif 'm' in num_str:
                return float(num_str.replace('m', '')) * 1000000
            else:
                return float(num_str)

        # Function to check if a string might be a price
        def is_likely_price(s):
            return '$' in s or any(word in s.lower() for word in ['price', 'offer', 'auction', 'guide', 'from'])

        # Extract price information
        price_pattern = r'\$?\s*([\d,.]+(?:\.\d+)?[km]?)(?:\s*-\s*\$?\s*([\d,.]+(?:\.\d+)?[km]?))?'
        matches = re.findall(price_pattern, price, re.IGNORECASE)
        
        valid_prices = [match for match in matches if is_likely_price(match[0])]
        
        if valid_prices:
            numbers = [convert_to_full_number(num) for match in valid_prices for num in match if num]
            
            if len(numbers) == 1:
                return pd.Series({'no_price_provided': False, 'point_estimate': numbers[0], 'lower_bound': numbers[0], 'upper_bound': numbers[0]})
            elif len(numbers) == 2:
                return pd.Series({'no_price_provided': False, 'point_estimate': None, 'lower_bound': min(numbers), 'upper_bound': max(numbers)})
        
        # Check for "offers over" or similar patterns
        offer_pattern = r'(?:from|over|above|starting|offers|guide)\s+\$?\s*([\d,.]+(?:\.\d+)?[km]?)'
        offer_match = re.search(offer_pattern, price, re.IGNORECASE)
        
        if offer_match:
            offer_price = convert_to_full_number(offer_match.group(1))
            return pd.Series({'no_price_provided': False, 'point_estimate': None, 'lower_bound': offer_price, 'upper_bound': None})
        
        # Check for non-price information
        if re.search(r'\b(contact|agent|auction|express|new|fresh|sale|just|listed|request|preview|negotiation)\b', price, re.IGNORECASE):
            return pd.Series({'no_price_provided': True, 'point_estimate': None, 'lower_bound': None, 'upper_bound': None})
        
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
output.to_csv('test_cases_with_additional_data_2.csv')