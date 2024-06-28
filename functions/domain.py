import yaml
import datetime


# Grabbing access token
def get_domain_access_token(session, client_creds_yml):
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
def residential_listings_search(access_token, 
                                request_session,
                                page_number = 1,
                                postcode = '',
                                state = '', 
                                region = '',
                                area = '', 
                                listing_type = 'Sale', 
                                updated_since = '',
                                listed_since = (datetime.datetime.today() - datetime.timedelta(days=1)).isoformat()
                    ):
    
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
                "field": "dateListed",
                "direction": "Ascending"
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
                                    listed_since= None): 
    
    max_pages_downloaded = False
    pageNumber = 0
    output = {}

    # Downloading up to 10 pages of listings
    while not (max_pages_downloaded | pageNumber >= 10):
        pageNumber  += 1

        download_date = dt.datetime.now(dt.timezone.utc).isoformat()
        
        request = residential_listings_search(
                                    access_token = access_token, 
                                    request_session = s,
                                    page_number = pageNumber,
                                    postcode = postcode,
                                    state = state, 
                                    region = region,
                                    area = area, 
                                    listing_type = listing_type, 
                                    updated_since = updated_since,
                                    listed_since= listed_since
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
        print('Page ' + str(pageNumber) + ' of ' + str(total_pages) + ' (' + str(len(listings)) + ' of ' + str(num_records) + ' total records downloaded)')
        
        max_pages_downloaded = pageNumber == total_pages

                # Checking that daily quotas have not been reached
        if pd.to_numeric(request.headers['X-Quota-PerDay-Remaining']) == 0:
            print('X-Quota-PerDay-Remaining = 0')
            max_pages_downloaded = True
        
        if not max_pages_downloaded:
            time.sleep(0.4)
    
    ## Cleanup and metadata
    ## Dealing with nested listed dates
    date_listeds = []
    for listing in listings:
        if listing['type'] == 'Project':
            for project_listings in listing['listings']:
                date_listeds.append(pd.to_datetime(project_listings.get('dateListed')))
        else: 
            date_listeds.append(pd.to_datetime(listing.get('listing').get('dateListed')))

    max_date_listed = pd.array(date_listeds).max()

    output = {
        'listings': listings,  
        'updated_since': updated_since,
        'listed_since_date': listed_since, 
        'download_date': download_date,
        'max_listed_since_date': max_date_listed,
        'daily_quota_remaining': pd.to_numeric(request.headers['X-Quota-PerDay-Remaining']),
        'pages_remaining': total_pages - pageNumber
    }

    return(output)