import datetime

def residential_listings_search(access_token, 
                                request_session,
                                page_number = 1,
                                postcode = '',
                                state = '', 
                                region = '',
                                area = '', 
                                listing_type = 'Sale', 
                                updated_since = (datetime.datetime.today() - datetime.timedelta(days=1)).isoformat()
                    ):
    
    auth = {"Authorization":"Bearer "+access_token}

    # Pulling together post fields
    post_fields = {
      'pageSize': 100,
      'pageNumber': page_number,
      "listingType": listing_type,
      "updatedSince": updated_since,
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
