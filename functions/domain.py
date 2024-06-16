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
