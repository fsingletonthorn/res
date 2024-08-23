import sqlite3

def drop_res_tables(filename):
    '''A function to drop the RES tables if you want to restart data collection without deleting the database file.
    Parameters
    ----------
    database_loc: sqlite database filepath
    '''
    conn = None
    try:
        conn = sqlite3.connect(filename)
        cur = conn.cursor()
        cur.execute("DROP TABLE download")
        cur.execute("DROP TABLE raw_listing;")
    except sqlite3.Error as e:
        print(e)
    finally:
        if conn:
            conn.execute("VACUUM")
            conn.close()

# drop_res_tables('res_database.db')

def create_sqlite_database(filename):
    """ create a database connection to an SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(filename)
        print(sqlite3.sqlite_version)
    except sqlite3.Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    create_sqlite_database("res_database.db")

def create_raw_listings_database(database_loc):
    """ create empty tables for raw data
    Parameters
    ----------
    database_loc: sqlite database filepath
    """

    conn = sqlite3.connect(database_loc)

    cur = conn.cursor()

    cur.execute("""
        create TABLE download (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            download_date TEXT, 
            listed_since_date TEXT,
            max_listed_since_date TEXT,
            listings TEXT,
            postcode TEXT,
            state TEXT,
            region TEXT,
            area TEXT,
            listing_type TEXT,
            updated_since TEXT,
            pages_remaining TEXT
        ); 
    """)

    cur.execute("""
            CREATE TABLE raw_listing (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                download_id INTEGER,
                type TEXT,
                download_date TEXT,
                project_state TEXT,
                project_id REAL,
                project_name TEXT,
                project_bannerUrl TEXT,
                project_preferredColorHex TEXT,
                project_logoUrl TEXT,
                project_labels TEXT,
                project_displayableAddress TEXT,
                project_suburb TEXT,
                project_features TEXT,
                project_media TEXT,
                project_projectSlug TEXT,
                listing_auctionSchedule_time TEXT,
                listing_auctionSchedule_auctionLocation TEXT,
                listing_listingType TEXT,
                listing_id REAL,
                listing_media TEXT,
                listing_headline TEXT,
                listing_summaryDescription TEXT,
                listing_hasFloorplan TEXT,
                listing_hasVideo TEXT,
                listing_labels TEXT,
                listing_dateListed TEXT,
                listing_listingSlug TEXT,
                listing_advertiser_type TEXT,
                listing_advertiser_id REAL,
                listing_advertiser_name TEXT,
                listing_advertiser_logoUrl TEXT,
                listing_advertiser_preferredColourHex TEXT,
                listing_advertiser_bannerUrl TEXT,
                listing_advertiser_contacts TEXT,
                listing_priceDetails_price REAL,
                listing_priceDetails_priceFrom REAL,
                listing_priceDetails_priceTo REAL,
                listing_priceDetails_displayPrice TEXT,
                listing_propertyDetails_state TEXT,
                listing_propertyDetails_features TEXT,
                listing_propertyDetails_propertyType TEXT,
                listing_propertyDetails_allPropertyTypes TEXT,
                listing_propertyDetails_bathrooms REAL,
                listing_propertyDetails_bedrooms REAL,
                listing_propertyDetails_carspaces REAL,
                listing_propertyDetails_unitNumber TEXT,
                listing_propertyDetails_streetNumber TEXT,
                listing_propertyDetails_street TEXT,
                listing_propertyDetails_area TEXT,
                listing_propertyDetails_region TEXT,
                listing_propertyDetails_suburb TEXT,
                listing_propertyDetails_postcode TEXT,
                listing_propertyDetails_displayableAddress TEXT,
                listing_propertyDetails_latitude REAL,
                listing_propertyDetails_longitude REAL,
                listing_propertyDetails_isRural TEXT,
                listing_propertyDetails_isNew TEXT,
                listing_propertyDetails_tags TEXT,
                listing_inspectionSchedule_byAppointment TEXT,
                listing_inspectionSchedule_recurring TEXT,
                listing_inspectionSchedule_times TEXT,
                listing_propertyDetails_landArea REAL,
                listing_propertyDetails_buildingArea REAL,
                FOREIGN KEY(download_id) REFERENCES download(id)
            ); 
    """)
      
    conn.commit()
    conn.close()

if __name__ == '__main__':
    create_raw_listings_database("res_database.db")




