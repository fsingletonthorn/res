import yaml
import datetime as dt
import pandas as pd
import math
import time
import pytz

# Grabbing access token
def get_domain_access_token(session, client_creds_yml):
    """
    A function to get your domain access token, with a requests session and your client credentials. 
    Returns 

    Parameters
    ----------
    session : requests.sessions.Session
        A requests session
    client_creds_yml : str
        Relative file path to your domain client credentials yml file, use the template at to create your own
    """
    with open(client_creds_yml, 'r') as f:
        doc = yaml.load(f, Loader=yaml.Loader)

    response = session.post('https://auth.domain.com.au/v1/connect/token', 
                    data = {'client_id':doc[0]['client_id'],
                            "client_secret":doc[0]['client_secret'],
                            "grant_type":"client_credentials",
                            "scope":"api_listings_read",
                            "Content-Type":"text/json"}
                    )

    token=response.json()
    access_token=token["access_token"]

    return(access_token)

## Residential listings search function
def rls_download_1_page(access_token, 
                                request_session,
                                page_number = 1,
                                postcode = '',
                                state = '', 
                                region = '',
                                area = '', 
                                listing_type = 'Sale', 
                                updated_since = '',
                                listed_since = (dt.datetime.today() - dt.timedelta(days=1)).isoformat(),
                                sort_on_field = 'dateListed',
                                sort_order = 'Ascending'
                    ):
    """
    A helper function that downloads a single page of results using the domain residential listings search api
    
    Parameters
    ----------
    access_token: str
        your domain API access token generated using get_domain_access_token()
    request_session : requests.sessions.Session
        An open requests session
    page_number: int, optional
        The page number to download, must be 1-10, default 1
    postcode: str, optional
        An Australian postcode as a string. If invalid or there are no listings will return no results.
    state : str, optional
    region : str, optional
    area : str, optional
    listing_type : str, optional
        Default = 'Sale' 
    updated_since: str, optional
        Updated since in iso format, Default = '', does not filter if left as ''
    listed_since: str, optional
        Listed since filter in iso format, Default = (dt.datetime.today() - dt.timedelta(days=1)).isoformat(), does not filter if left as ''
    sort_on_field: str, optional
        Field to use for sorting, default: 'dateListed'. Options: [ Default, Suburb, Price, DateUpdated, InspectionTime, AuctionTime, Proximity, SoldDate, DefaultThenDateUpdated, DateAvailable, DateListed ]
    sort_order: str, optional
        Sort order, either 'Ascending' or 'Descending', default = 'Ascending'
    """

    auth = {"Authorization":"Bearer "+access_token}

    # Pulling together post fields
    post_fields = {
        'pageSize': 100,
        'pageNumber': page_number,
        "listingType": listing_type,
        "updatedSince": updated_since,
        "listedSince": listed_since,
        "customSort": {
            "elements": [
            {
                "field": sort_on_field,
                "direction": sort_order
            }
            ],
            "boostPrimarySuburbs": False
        },
        "locations":[
            {
            "state":state,
            "region":region,
            "area":area,
            "postCode":postcode,
            "includeSurroundingSuburbs":False
            }
        ]
        }
    
    url = "https://api.domain.com.au/v1/listings/residential/_search"

    request = request_session.post(url,headers=auth,json=post_fields)

    return request

## Loop to download 10 pages and gather metadata on listings
def rls_download_10_pages(access_token = None, 
                                    request_session = None,
                                    postcode = None,
                                    state = None,
                                    region = None,
                                    area = None,
                                    listing_type = None,
                                    updated_since = None,
                                    listed_since= None,
                                    sort_on_field = 'dateListed',
                                    sort_order = 'Ascending'
                                    ): 
    """
    A helper function that downloads a 10 page of results using the domain residential listings search api and the rls_download_1_page function.
    Essentially just a wrapper around that function that loops through up to 10 results and returns: 
        output = {
        'listings': listings,  
        'updated_since': updated_since,
        'listed_since_date': listed_since, 
        'download_date': download_date,
        'max_listed_since_date': max_date_listed,
        'daily_quota_remaining': pd.to_numeric(request.headers['X-Quota-PerDay-Remaining']),
        'pages_remaining': total_pages - pageNumber
    }
    
    Parameters
    ----------
    access_token: str
        your domain API access token generated using get_domain_access_token()
    request_session : requests.sessions.Session
        An open requests session
    page_number: int, optional
        The page number to download, must be 1-10, default 1
    postcode: str, optional
        An Australian postcode as a string. If invalid or there are no listings will return no results.
    state : str, optional
    region : str, optional
    area : str, optional
    listing_type : str, optional
        Default = 'Sale' 
    updated_since: str, optional
        Updated since in iso format, Default = '', does not filter if left as ''
    listed_since: str, optional
        Listed since filter in iso format, Default = (dt.datetime.today() - dt.timedelta(days=1)).isoformat(), does not filter if left as ''
    sort_on_field: str, optional
        Field to use for sorting, default: 'dateListed'. [ Default, Suburb, Price, DateUpdated, InspectionTime, AuctionTime, Proximity, SoldDate, DefaultThenDateUpdated, DateAvailable, DateListed ]
    sort_order: str, optional
        Sort order, either 'Ascending' or 'Descending', default = 'Ascending'
    """
    max_pages_downloaded = False
    pageNumber = 0
    output = {}

    # Downloading up to 10 pages of listings
    while not (max_pages_downloaded | (pageNumber >= 10)):
        pageNumber  += 1

        tz = pytz.timezone('Australia/Sydney')
        download_date = dt.datetime.now(tz).isoformat()
        
        request = rls_download_1_page(access_token = access_token, 
                                    request_session = request_session,
                                    page_number = pageNumber,
                                    postcode = postcode,
                                    state = state, 
                                    region = region,
                                    area = area, 
                                    listing_type = listing_type, 
                                    updated_since = updated_since,
                                    listed_since= listed_since,
                                    sort_on_field = sort_on_field,
                                    sort_order = sort_order
                                    )
        
        # Error out if status code not 200
        if request.status_code != 200:
            raise ValueError('Error: Request status code request was: ' + str(request.status_code) + ', not 200')

        # Counting number of pages required here
        if pageNumber == 1:
            
            # Calculations for number of loops
            page_size = pd.to_numeric(request.headers['X-Pagination-PageSize'])
            num_records = pd.to_numeric(request.headers['X-Total-Count'])
            total_pages = math.ceil(num_records / page_size)

            # Checking that we are not going beyond the pagination limit
        # if total_pages > 10:
        #     raise ValueError('Error: Request returns: ' + str(num_records) + ', total records, narrow search parameters to only include 1000 listings at most to include all listings')

            # Checking that we are not going beyond the pagination limit
            if num_records == 0:
                raise ValueError('Error: Request returns: ' + str(num_records) + ' records, check search parameters')
    
        # Getting Json responses
        request_json = request.json()

        # # Inserting downloaded date
        for i in range(len(request_json)): 
            request_json[i]['download_date'] = download_date

        # Saving or extending json
        if pageNumber == 1:
            listings = request_json
        else:
            listings.extend(request_json)

        # For logging
        print('Page ' + str(pageNumber) + ' of ' + str(total_pages) + ' (' + str(len(listings)) + ' of ' + str(num_records) + ' remaining records records downloaded)')
        
        max_pages_downloaded = pageNumber == total_pages

                # Checking that daily quotas have not been reached
        if pd.to_numeric(request.headers['X-Quota-PerDay-Remaining']) == 0:
            print('X-Quota-PerDay-Remaining = 0')
            max_pages_downloaded = True
        
        if not max_pages_downloaded:
            time.sleep(0.5)
    
    ## Cleanup and metadata
    ## Dealing with nested listed dates
    date_listeds = []
    for listing in listings:
        if listing['type'] == 'Project':
            ## We have to ignore the date listeds in projects - although they will get downloaded multiple times
            ## As the sort by listed date doesn't appear to work for projects (bug in API?)
            next
            # for project_listings in listing['listings']:
            #     date_listeds.append(pd.to_datetime(project_listings.get('dateListed')))
        else: 
            date_listeds.append(pd.to_datetime(listing.get('listing').get('dateListed')))

    max_date_listed = pd.array(date_listeds).max()

    output = {
        'listings': listings,  
        'updated_since': updated_since,
        'listed_since_date': listed_since, 
        'download_date': download_date,
        'max_listed_since_date': max_date_listed.isoformat(),
        'daily_quota_remaining': pd.to_numeric(request.headers['X-Quota-PerDay-Remaining']),
        'pages_remaining': total_pages - pageNumber
    }

    return(output)

## Loop to download 10 pages and gather metadata on listings
def residential_listings_search(access_token = None, 
                                    request_session = None,
                                    postcode = None,
                                    state = None,
                                    region = None,
                                    area = None,
                                    listing_type = None,
                                    updated_since = None,
                                    listed_since= None,
                                    sort_on_field = 'dateListed',
                                    sort_order = 'Ascending'
                                    ):
    """
    A helper function that loops through and downloads all results using the domain residential listings search api and the rls_download_10_page function.
    Essentially just a wrapper around that function that loops through and downloads results until all are downloaded, or until you run out of tokens.

    Function returns a list that contains the first listed since date used, and the max listed date: 
    output = {
        'listed_since_date': original listed since date
        'max_listed_since_date': max returned listed date
        'listings': listings - a dict of listings
        'postcode': postcode - original provided by function for tracking
        'state': state - original provided by function for tracking
        'region': region - original provided by function for tracking
        'area': area- original provided by function for tracking
        'listing_type': listing_type - original provided by function for tracking
        'updated_since': updated_since- original provided by function for tracking 
        'pages_remaining': pages of results remaining when function ends
    }
    
    Parameters
    ----------
    access_token: str
        your domain API access token generated using get_domain_access_token()
    request_session : requests.sessions.Session
        An open requests session
    page_number: int, optional
        The page number to download, must be 1-10, default 1
    postcode: str, optional
        An Australian postcode as a string. If invalid or there are no listings will return no results.
    state : str, optional
    region : str, optional
    area : str, optional
    listing_type : str, optional
        Default = 'Sale' 
    updated_since: str, optional
        Updated since in iso format, Default = '', does not filter if left as ''
    listed_since: str, optional
        Listed since filter in iso format, Default = (dt.datetime.today() - dt.timedelta(days=1)).isoformat(), does not filter if left as ''
    """
    # Loops through listings and uses

    download_complete = False
    listings_list = list()
    original_listed_since = listed_since

    while not download_complete:
        # Initialising variables

        listings_w_meta = rls_download_10_pages(access_token = access_token, 
                                            request_session = request_session,
                                            postcode = postcode,
                                            state = state, 
                                            region = region,
                                            area = area, 
                                            listing_type = listing_type, 
                                            updated_since = updated_since,
                                            listed_since= listed_since
                                    )

        listings_list.append(listings_w_meta)

        print("Pages remaining: " + str(listings_w_meta['pages_remaining']) + ', Daily quota remaining: ' + str(listings_w_meta['daily_quota_remaining']) )  
        if (listings_w_meta['pages_remaining'] == 0) | (listings_w_meta['daily_quota_remaining'] == 0):
            download_complete = True
        else:
            listed_since = listings_w_meta['max_listed_since_date']
  
    ## dict method to dedupe from listings and make a big output listing dict
    listings_dict = dict()
    for listings in listings_list:
        for x in listings['listings']:
            if (x['type'] == 'Project'):
                listings_dict[f"project_{x['project']['id']}"] = x
            if (x['type'] == 'PropertyListing'):
                listings_dict[f"listing_{x['listing']['id']}"] = x

    output = {
        'listed_since_date': original_listed_since,
        'max_listed_since_date': listings_list[len(listings_list)-1]['max_listed_since_date'],
        'listings': listings_dict,
        'postcode': postcode,
        'state': state,
        'region': region,
        'area': area,
        'listing_type': listing_type,
        'updated_since': updated_since,
        'pages_remaining': listings_list[len(listings_list)-1]['pages_remaining']
    }

    return(output)