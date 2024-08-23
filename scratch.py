
## Loop to download 10 pages and gather metadata on listings
def residential_listings_search(access_token = None, 
                                    request_session = None,
                                    postcode = None,
                                    state = None,
                                    region = None,
                                    area = None,
                                    listing_type = None,
                                    updated_since = None,
                                    listed_since= None):
    """
    A helper function that loops through and downloads all results using the domain residential listings search api and the rls_download_10_page function.
    Essentially just a wrapper around that function that loops through and downloads results until all are downloaded, or until you run out of tokens.

    Function returns a list that contains the first listed since date used, and the max listed date: 
    output = {
        'listed_since_date': original listed since date
        'max_listed_since_date': max returned listed date
        'listings': listings - list of dicts containing listings
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
                                            listed_since= listed_since)

        listings_list.append(listings_w_meta)

        print("Pages remaining: " + str(listings_w_meta['pages_remaining']) + ', Daily quota remaining: ' + str(listings_w_meta['daily_quota_remaining']) )  
        if (listings_w_meta['pages_remaining'] == 0) | (listings_w_meta['daily_quota_remaining'] == 0):
            download_complete = True
        else:
            listed_since = listings_w_meta['max_listed_since_date']
  
    
    listings = list()

    # Putting all listings into a list
    for x in listings_list:
       listings.extend(x['listings'])

    ## Probably use the listings dict method to dedupe from the below
    listings_dict = dict()
    ids = list()
    for x in listings:
        if (x['type'] == 'Project'):
            listings_dict[f"project_{x['project']['id']}_{x['project']['id']}"] = x
            ids.append((f"project_{x['project']['id']}"))
            print(f"project_{x['project']['id']}")
        if (x['type'] == 'PropertyListing'):
            listings_dict[f"listing_{x['listing']['id']}"] = x
            ids.append(f"listing_{x['listing']['id']}")
            print(f"listing_{x['listing']['id']}")

    listings_dict['project_5861']

    for x in listings:
        # if (x['type'] == 'PropertyListing'):
        #     if f"listing_{x['listing']['id']}" == 'listing_2019350948': 
        #         print(x)

        if (x['type'] == 'Project'):
            if f"project_{x['project']['id']}" == 'project_5861':
               print(x, '        xxx       ')
 

    ids = pd.DataFrame(ids)

    ids[(ids.duplicated())]

    deduped_listings = list()
    # Takes ages
    [deduped_listings.append(listing) for listing in listings if listing not in deduped_listings]

    len(deduped_listings)
    # moving the list into a named dict to remove duplicates

    len(listings)
    len(listings_dict)
    len(deduped_listings)        
    
    listings[8]

    output = {
        'listed_since_date': listings_list[0]['listed_since_date'],
        'max_listed_since_date': listings_list[len(listings_list)-1]['max_listed_since_date'],
        'listings': listings,
        'postcode': postcode,
        'state': state,
        'region': region,
        'area': area,
        'listing_type': listing_type,
        'updated_since': updated_since,
        'pages_remaining': listings_list[len(listings_list)-1]['pages_remaining']
    }

    return(output)