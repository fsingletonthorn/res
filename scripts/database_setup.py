import duckdb
import os
from functions.database import create_md_connection

def motherduck_truncate_tables(token = os.environ["MOTHERDUCK_TOKEN"]):
    '''
    Warning - this will drop all data.
    '''
    conn = create_md_connection(token)
    conn.sql('truncate table res.raw.sale_listing')
    conn.sql('truncate table res.raw.sold_listing')
    conn.sql('truncate table res.raw.download')

def motherduck_setup_raw(token = os.environ["MOTHERDUCK_TOKEN"]):
    conn = create_md_connection(token)
    conn.sql('create database res')
    conn.sql('use res')

    conn.sql('create schema res.raw')

    ## Creating autoincrements 
    conn.sql('CREATE or replace SEQUENCE res.raw.seq_download_pk START 1;')
    conn.sql('CREATE or replace SEQUENCE res.raw.seq_sale_listing_pk START 1;')
    conn.sql('CREATE or replace SEQUENCE res.raw.seq_sold_listing_pk START 1;')

    conn.sql("""
        create TABLE res.raw.download (
            id INTEGER PRIMARY KEY default nextval('raw.seq_download_pk'),
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

    conn.sql("""
            CREATE TABLE res.raw.sale_listing (
                id INTEGER PRIMARY KEY default nextval('raw.seq_sale_listing_pk'),
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
                FOREIGN KEY(download_id) REFERENCES raw.download(id)
            ); 
    """)

    conn.sql("""
            CREATE TABLE res.raw.sold_listing (
                id INTEGER PRIMARY KEY default nextval('raw.seq_sold_listing_pk'), 
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
                listing_soldData_source TEXT,
                listing_soldData_saleMethod TEXT,
                listing_soldData_soldDate TEXT,
                listing_soldData_soldPrice REAL,
                FOREIGN KEY(download_id) REFERENCES raw.download(id)
            ); 
        """)

    conn.commit()
    conn.close()
