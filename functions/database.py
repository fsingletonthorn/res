import sqlite3
import pandas as pd
import datetime as dt
import pytz

# Grab last uploaded date: 
## Get parameters
def get_last_download_metadata(database_loc, listing_type = 'Sale'):
    """
    A helper function that downloads the latest results from the download table containing metadata on the downloads that have been completed by RES.
    
    Parameters
    ----------
    database_loc: str
        The filepath to your database including file extension. 
    listing_type: str
        Whether to download Sale listings or Sold properties. Enter either 'Sold' or 'Sale'. Injected straight into the sql query string so user beware.
        Default: Sale
    """
    conn = sqlite3.connect(database_loc)

    out = pd.read_sql(f'SELECT * FROM download where listing_type = "{listing_type}" ORDER BY id DESC LIMIT 1;', conn)
    conn.close()
    return(out)

def get_metadata(database_loc):
    """
    A helper function that downloads the download table containing metadata on the downloads that have been completed by RES.
    
    Parameters
    ----------
    database_loc: str
        The filepath to your database including file extension. 
    """
    conn = sqlite3.connect(database_loc)
    out = pd.read_sql('SELECT * FROM download;', conn)
    conn.close()
    return(out)

def get_raw_sale_listings(database_loc):
    """
    A helper function that downloads the download table containing metadata on the downloads that have been completed by RES.
    
    Parameters
    ----------
    database_loc: str
        The filepath to your database including file extension. 
    """
    conn = sqlite3.connect(database_loc)
    out = pd.read_sql('SELECT * FROM raw_sale_listing;', conn)
    conn.close()
    return(out)

def get_raw_sold_listings(database_loc):
    """
    A helper function that downloads the download table containing metadata on the downloads that have been completed by RES.
    
    Parameters
    ----------
    database_loc: str
        The filepath to your database including file extension. 
    """
    conn = sqlite3.connect(database_loc)
    out = pd.read_sql('SELECT * FROM raw_sold_listing;', conn)
    conn.close()
    return(out)

def update_listings_tables(raw_output, cleaned_listings):
    conn = sqlite3.connect("res_database.db")
    cur = conn.cursor()

    tz = pytz.timezone('Australia/Sydney')
    sydney_now = dt.datetime.now(tz)
    listed_since_date = sydney_now

    download_meta = pd.DataFrame({
        'download_date': [listed_since_date.isoformat()], 
        'listed_since_date': [raw_output['listed_since_date']],
        'max_listed_since_date': [raw_output['max_listed_since_date']],
        'postcode': [raw_output['postcode']],
        'state': [raw_output['state']],
        'region': [raw_output['region']],
        'area': [raw_output['area']],
        'listing_type': [raw_output['listing_type']],
        'updated_since': [raw_output['updated_since']],
        'pages_remaining': [raw_output['pages_remaining']]
        })

    download_meta.to_sql('download', conn, if_exists='append', index=False)

    # pd.read_sql('select * from download;', conn)

    max_download_id = pd.read_sql('select max(id) as download_id from download;', conn)

    ## Striping object types of
    all_listings_for_upload = cleaned_listings.convert_dtypes().infer_objects()
    all_listings_for_upload.update(all_listings_for_upload.select_dtypes('object').astype(str))
    all_listings_for_upload['download_id'] = max_download_id['download_id'][0]

    if download_meta.listing_type[0] == 'Sale': 
        all_listings_for_upload.to_sql('raw_sale_listing', conn, if_exists='append', index=False)
    elif download_meta.listing_type[0] == 'Sold':
        all_listings_for_upload.to_sql('raw_sold_listing', conn, if_exists='append', index=False)
    else:
        raise ValueError('download_meta.listing_type not recognized: "' + str(download_meta.listing_type) + '", not "Sale" or "Sold"')        

    conn.execute("VACUUM")
    conn.commit()

    cur.close()
