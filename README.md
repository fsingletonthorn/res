# RES
A tiny repo that pulls real estate listings and sales to understand which real estate agencies tend to underquote. Currently under development. 

## How to run this locally
To access the Domain APIs you will need to first sign up for a developer account at https://developer.domain.com.au/.

Then, create a new project at https://developer.domain.com.au/projects/,  and set up credentials for your project in the Projects tab. The easiest way to consistently access this project - although not the most secure - is via a simple API key.

Once you have created an API key, then create a file in the root directly of this project called `client_credentials.yml`, providing your client credential and client secrete as per  `client_credentials_template.yml`.

If you want to start with a blank slate you can either delete  `res_database.db` and run all the scripts in `scripts\sqlite_setup.py`.

Project planning: 
- Decide on cloud provider
    - [ ] Google Cloud
    - [ ] AWS
    - [x] Mock up a "database" -SQLlite, but first dump into json files to ingest later (i.e., data lake approach)
- [ ] Data pipeline setup? Dbt? Could use an alternative. Total overkill but fun :)
- [x] Decide on API access method - requests direct pings 
- [ ] Decide on workflow and code up
    - [x] Access up to 1000 pages per day
    - [x] Order by listed date
    - [x] Download from latest listing date to 1000 pages, then resume downloading from last listed date next day. Error out and stop if there are no records to download. 
- [x] Write up download script and connect to data store
- [ ] Setup Github actions to orchestrate
- [ ] Pull sales data for properties as appropriate
    - [ ] Decide on approach - e.g., take long list, ping each that hasn't been checked in x days, that has been up for at least x days


- Plan: 
-- Select all records that were updated after the last listed date from the last download
-- Store all records
-- This will create some duplicates due to issues with the last listed date in the database
-- Not necessarily a problem, will ensure that we're capturing all records and we can dedupe later