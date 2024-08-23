import sqlite3
import pandas as pd

# Grab last uploaded date: 
## Get parameters
def get_last_download_metadata(database_loc):
    """
    A helper function that downloads the latest results from the download table containing metadata on the downloads that have been completed by RES.
    
    Parameters
    ----------
    database_loc: str
        The filepath to your database including file extension. 
    """
    conn = sqlite3.connect(database_loc)
    out = pd.read_sql('SELECT * FROM download ORDER BY id DESC LIMIT 1;', conn)
    conn.close()
    return(out)
