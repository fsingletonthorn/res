# RES
A tiny repo that allows access to residential real estate listings and sales via the domain API. Currently under development. 

## How to run this locally
To access the Domain APIs you will need to first sign up for a developer account at https://developer.domain.com.au/.

Then, create a new project at https://developer.domain.com.au/projects/,  and set up credentials for your project in the Projects tab. The easiest way to consistently access this project - although not the most secure - is via a simple API key. You will need access to the Agents & Listings API to be able to run this repo's code.

Once you have created an API key, then add your `client_id` and `client_secret` to your environment variables to access the Domain API using this repo (note, this may incur costs, although the free tier should provide sufficient access to run this daily without exceeding rate limits).

This repo is set up to work with Motherduck, you can set up a free account, add your MOTHERDUCK_TOKEN as an environment variable, and run the `motherduck_setup_raw` function in `scripts\database_setup.py` to setup an empty database with the necessary schemas and tables to run this repo.

### Validation of price extraction code
A sample of 250 prices was extracted randomly from the dataset in Nov. 2024 to create the extract_price_info function which extracts listing price guide figures from the free text prices in the listing price field. Out of this sample, 247 prices or price ranges were correctly identified. The correctly extracted prices included 101 examples where there was no reported price, 146 accurate prices, price ranges, or price lower bounds (e.g., "Offers from $1.3m"). There were 3 were incorrect extractions in this sample; one price reported as "1.95 mill" which was not extracted, one that was  "FROM MID $800,000's" which was extracted as a single price at $800,000, and one that was advertised as "Interest around $500k" which was extracted as a single price of $500,000.