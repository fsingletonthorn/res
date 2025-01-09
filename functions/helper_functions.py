import pandas as pd
import re

def clean_listings(residential_listings_search_output, debug = False):
    """
    A function to take residential_listings_search() output and convert it into a flat table.
    
    Parameters
    ----------
    residential_listings_search_output: obj
        The results of residential_listings_search(...) to b cleaned
    """
    if debug:
        print('Total listings: '+ str(len(residential_listings_search_output['listings'])))
        print('Total unique listings by string: ' + str(len(set(residential_listings_search_output['listings'].keys()))))

    # Now dealing with the nested data
    listings = [value for value in residential_listings_search_output['listings'].values()]

    listings_json = pd.json_normalize(listings)

    projects = listings_json.loc[listings_json['type'] == 'Project']

    property_listings = listings_json.loc[listings_json['type'] == 'PropertyListing']

    if len(projects) > 0:
        ## Need to deal with projects that have nested json even after normalization - pd.json_normalize(test.loc[test['type'] == 'Project']['listings'][0])
        project_listings = pd.concat(list(map(pd.json_normalize, projects['listings']))).add_prefix('listing.')

        ## Getting all project data that are not dupes
        project_metadata = projects[projects.columns[~projects.columns.isin(project_listings.columns)]]
        
        project_metadata = project_metadata.drop_duplicates(subset='project.id')

        project_listings_w_meta = project_metadata.merge(project_listings, left_on='project.id', right_on='listing.projectId', how = 'left', validate = 'one_to_many')

        ## Dropping dupe col
        project_listings_w_meta = project_listings_w_meta.drop('listing.projectId', axis = 1)

        if debug:
            print('Debug - Project listing ids duplicated: ' + str(project_listings_w_meta['listing.id'].duplicated().sum()))

        # Concatinating all listings together, dropping nested listings col (now joined on)
        all_listings = pd.concat([project_listings_w_meta, property_listings], ignore_index = True).drop('listings', axis = 1)

    elif any(property_listings.columns == 'listings'):
        all_listings = property_listings.drop('listings', axis = 1)
    else: 
        all_listings = property_listings
    
    if debug:
        print('Debug - all listing ids duplicated: ' + str(all_listings['listing.id'].duplicated().sum()))

    all_listings.columns = (all_listings.columns
                    .str.replace('.', '_', regex=False)
                )
    
    return(all_listings)


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