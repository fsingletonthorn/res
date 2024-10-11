# Grabbing access token
import yaml
import requests
import json

def get_domain_access_token(session, client_id, client_secret):
    """
    Get a Domain API access token using client credentials.

    Parameters
    ----------
    session : requests.Session
        An active requests session.
    client_creds_yml : str
        Path to the YAML file containing client credentials.

    Returns
    -------
    str
        The access token.

    Raises
    ------
    ValueError
        If the credentials file is invalid or the token request fails.
    """
    try: 
        response = session.post(
            'https://auth.domain.com.au/v1/connect/token',
            data={
                'client_id': client_id,
                'client_secret': client_secret,
                'grant_type': 'client_credentials',
                'scope': 'api_listings_read',
                'Content-Type': 'text/json'
            }
        )
        response.raise_for_status()
        return response.json()['access_token']
    except (requests.RequestException) as e:
        raise ValueError(f"Failed to obtain access token: {str(e)}")

import requests
import pandas as pd
import datetime as dt
import pytz
import time
from typing import Dict, Any, List

def residential_listings_search(
    access_token: str,
    request_session: requests.Session,
    postcode: str = '',
    state: str = '',
    region: str = '',
    area: str = '',
    listing_type: str = 'Sale',
    updated_since: str = '',
    listed_since: str = '',
    verbose: bool = True,
    debug: bool = False
) -> Dict[str, Any]:
    """
    Search for residential listings using the Domain API, downloading all available results.

    This function combines the functionality of rls_download_1_page, rls_download_10_pages,
    and the original residential_listings_search into a single, more efficient function.

    Parameters
    ----------
    access_token : str
        Domain API access token.
    request_session : requests.Session
        An active requests session.
    postcode : str, optional
        Australian postcode to search.
    state : str, optional
        State to search.
    region : str, optional
        Region to search.
    area : str, optional
        Area to search.
    listing_type : str, optional
        Type of listing (default is 'Sale').
    updated_since : str, optional
        Filter for listings updated since this date (ISO format).
    listed_since : str, optional
        Filter for listings listed since this date (ISO format).
    sort_on_field : str, optional
        Field to sort results by (default is 'dateListed').
    sort_order : str, optional
        Sort order, either 'Ascending' or 'Descending' (default is 'Ascending').
    verbose: bool, optional
        Whether to print download counts and remaining download counts. 
        
    Returns
    -------
    Dict[str, Any]
        Dictionary containing all listings and metadata.
    """

    tz = pytz.timezone('Australia/Sydney')
    batch_listed_since = listed_since

    def download_page(page_number: int, batch_listed_since: str) -> requests.Response:
        url = "https://api.domain.com.au/v1/listings/residential/_search"
        headers = {"Authorization": f"Bearer {access_token}"}
        payload = {
            'pageSize': 100,
            'pageNumber': page_number,
            "listingType": listing_type,
            "updatedSince": updated_since,
            ## Going back one second to make sure we don't miss things
            "listedSince": batch_listed_since,
            "customSort": {
                "elements": [{"field": 'dateListed', "direction": 'Ascending'}],
                "boostPrimarySuburbs": False
            },
            "locations": [{
                "state": state,
                "region": region,
                "area": area,
                "postCode": postcode,
                "includeSurroundingSuburbs": False
            }]
        }
        return request_session.post(url, headers=headers, json=payload)

    all_listings: List[Dict[str, Any]] = []
    max_date_listed = None
    total_count = 0
    overall_page_number = 0
    
    while True:
        batch_listings = []

        for page_in_batch in range(1, 11):  # API limit of 10 pages per query
            overall_page_number += 1
            response = download_page(page_in_batch, batch_listed_since)
            response.raise_for_status()
            
            current_total_count = int(response.headers['X-Total-Count'])
            if current_total_count > total_count:
                total_count = current_total_count
            
            if overall_page_number == 1 and total_count == 0:
                raise ValueError('Error: Request returns 0 records, check search parameters')
            
            listings = response.json()
            last_download_date = dt.datetime.now(tz).isoformat()
            for listing in listings:
                listing['download_date'] = last_download_date
            batch_listings.extend(listings)
            
            records_downloaded = len(all_listings) + len(batch_listings)
            remaining_records = max(0, total_count - records_downloaded)
            total_pages = -(-total_count // int(response.headers['X-Pagination-PageSize']))  # Ceiling division
            
            if verbose:
               print(f'Page {overall_page_number} of {total_pages} ({records_downloaded} of {total_count} records downloaded, {remaining_records} remaining)')
            
            if int(response.headers['X-Pagination-PageSize']) * page_in_batch >= current_total_count or int(response.headers['X-Quota-PerDay-Remaining']) == 0:
                break
            
            time.sleep(0.5)  # Rate limiting
        
        # Process the batch
        for listing in batch_listings:
            if listing['type'] != 'Project':
                date_listed = pd.to_datetime(listing.get('listing', {}).get('dateListed'))
                if date_listed:
                    max_date_listed = max(max_date_listed, date_listed) if max_date_listed else date_listed
        
        all_listings.extend(batch_listings)
        
        pages_remaining = max(0, total_pages - overall_page_number)
        daily_quota_remaining = int(response.headers['X-Quota-PerDay-Remaining'])

        if verbose: 
            print(f'Pages remaining: {pages_remaining}, Daily quota remaining: {daily_quota_remaining}')
        
        # Check if we've retrieved all listings
        if len(all_listings) >= total_count or daily_quota_remaining == 0:
            break
        
        # Update listed_since for the next batch
        if max_date_listed:
            batch_listed_since = max_date_listed.isoformat()
    
    # Deduplicate listings
    if debug: 
        listings_dict = {}
        duplicate_count = 0

        for item in all_listings:
            if item['type'] == 'Project':
                key = f"project_{item['project']['id']}"
            elif item['type'] == 'PropertyListing':
                key = f"listing_{item['listing']['id']}"
            else:
                continue  # Skip unknown types

            if key in listings_dict:
                duplicate_count += 1
                print(f"Duplicate found for {key}")
                print("Original:", json.dumps(listings_dict[key], indent=2))
                print("Duplicate:", json.dumps(item, indent=2))
                print("Differences:")
                for k in set(listings_dict[key].keys()) | set(item.keys()):
                    if k not in listings_dict[key] or k not in item or listings_dict[key][k] != item[k]:
                        print(f"  {k}:")
                        print(f"    Original: {listings_dict[key].get(k)}")
                        print(f"    Duplicate: {item.get(k)}")
            
            listings_dict[key] = item
        print(f"Total duplicates found: {duplicate_count}")
    else:
        listings_dict = {}
        for item in all_listings:
            if item['type'] == 'Project':
                listings_dict[f"project_{item['project']['id']}"] = item
            elif item['type'] == 'PropertyListing':
                listings_dict[f"listing_{item['listing']['id']}"] = item

    
    return {
        'listed_since_date': listed_since,
        'max_listed_since_date': max_date_listed.isoformat() if max_date_listed else None,
        'max_download_date': last_download_date,
        'listings': listings_dict,
        'postcode': postcode,
        'state': state,
        'region': region,
        'area': area,
        'listing_type': listing_type,
        'updated_since': updated_since,
        'pages_remaining': pages_remaining,
        'total_listings_retrieved': len(all_listings),
        'total_unique_listings': len(listings_dict),
        'total_pages_processed': overall_page_number
    }