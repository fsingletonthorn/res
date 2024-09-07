import duckdb
import pandas as pd
import datetime as dt
import pytz
import os

def create_md_connection(token):
    con = duckdb.connect(f'md:?motherduck_token={token}') 
    con.sql("use res")
    return con

# Grab last uploaded date: 
## Get parameters
def get_last_download_metadata(listing_type = 'Sale', md_token = os.environ["MOTHERDUCK_TOKEN"]):
    """
    A helper function that downloads the latest results from the download table containing metadata on the downloads that have been completed by RES.
    
    Parameters
    ----------
    listing_type: str
        Whether to download Sale listings or Sold properties. Enter either 'Sold' or 'Sale'. Injected straight into the sql query string so user beware.
        Default: Sale
    md_token: str
        Motherduck access token
    """
    conn = create_md_connection(md_token)

    out = conn.sql(f"SELECT * FROM res.raw.download where listing_type = '{listing_type}' ORDER BY id DESC LIMIT 1;").to_df()
    conn.close()
    return(out)

def get_metadata(md_token = os.environ["MOTHERDUCK_TOKEN"]):
    """
    A helper function that downloads the download table containing metadata on the downloads that have been completed by RES.
    
    Parameters
    ----------
    md_token: str
        A Motherduck token to connect to the Motherduck db
    """
    conn = create_md_connection(md_token)
    out = conn.sql('SELECT * FROM res.raw.download;').to_df()
    conn.close()
    return(out)

def get_raw_sale_listings(md_token = os.environ["MOTHERDUCK_TOKEN"]):
    """
    A helper function that downloads the sale listing table containing metadata on the downloads that have been completed by RES.
    
    Parameters
    ----------
    md_token: str
        A Motherduck token to connect to the Motherduck db
    """
    conn = create_md_connection(md_token)
    out = conn.sql('SELECT * FROM res.raw.sale_listing;').to_df()
    conn.close()
    return(out)

def get_raw_sold_listings(md_token = os.environ["MOTHERDUCK_TOKEN"]):
    """
    A helper function that downloads the sold listings table containing metadata on the downloads that have been completed by RES.
    
    Parameters
    ----------
    md_token: str
        A Motherduck token to connect to the Motherduck db
    """
    conn = create_md_connection(md_token)
    out = conn.sql('SELECT * FROM res.raw.sold_listing;').to_df()
    conn.close()
    return(out)

def update_listings_tables(raw_output, cleaned_listings, md_token=os.environ["MOTHERDUCK_TOKEN"]):
    """
    This function inserts download metadata and cleaned listings into appropriate
    tables based on the listing type (Sale or Sold).

    Parameters
    ----------
    raw_output : dict
        A dictionary containing metadata about the download, including:
        listed_since_date, max_listed_since_date, postcode, state, region,
        area, listing_type, updated_since, and pages_remaining.
    cleaned_listings : pandas.DataFrame
        A DataFrame containing the cleaned listings data to be inserted.
    md_token : str, optional
        The MotherDuck token for database connection. Defaults to the
        "MOTHERDUCK_TOKEN" environment variable.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the listing_type in raw_output is neither 'Sale' nor 'Sold'.

    Notes
    -----
    - Requires tables res.raw.download, res.raw.sale_listing, and 
    res.raw.sold_listing to exist in the connected DuckDB database. 
    These can be created using the database setup script - scripts\database_setup.py.
    - Uses "INSERT INTO ... BY NAME" SQL syntax, requiring DataFrame column 
    names to match table column names.
    """
    conn = create_md_connection(md_token)

    download_meta = pd.DataFrame({
        'download_date': [raw_output['max_download_date']], 
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

    conn.sql("INSERT INTO res.raw.download BY NAME SELECT * FROM download_meta")
    max_download_id = conn.sql("SELECT max(id) as download_id FROM res.raw.download;").df()

    all_listings_for_upload = cleaned_listings.convert_dtypes().infer_objects()
    all_listings_for_upload.update(all_listings_for_upload.select_dtypes('object').astype(str))
    all_listings_for_upload['download_id'] = max_download_id['download_id'][0]

    if download_meta.listing_type[0] == 'Sale': 
        conn.sql("INSERT INTO res.raw.sale_listing BY NAME SELECT * FROM all_listings_for_upload")
    elif download_meta.listing_type[0] == 'Sold':
        conn.sql("INSERT INTO res.raw.sold_listing BY NAME SELECT * FROM all_listings_for_upload")
    else:
        raise ValueError(f'download_meta.listing_type not recognized: "{download_meta.listing_type[0]}", not "Sale" or "Sold"')

    conn.execute("VACUUM")
    conn.commit()

    conn.close()
