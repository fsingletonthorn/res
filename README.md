Goals: Use publicly available real estate listing data to understand which real estate agencies tend to underquote. 

To access the Domain APIs you will need to first sign up for a developer account at https://developer.domain.com.au/.

Then, create a new project at https://developer.domain.com.au/projects/,  and set up credentials for your project in the Projects tab. The easiest way to consistently access this project - although not the most secure - is via a simple API key.

Once you have created an API key, then create a file in the root directly of this project called `client_credentials.yml`, providing your client credential and client secrete as per  `client_credentials_template.yml`.

Project planning: 
- Decide on cloud provider
- [ ] Google Cloud
- [ ] AWS
- [ ] Mock up a database on e.g., googlesheets to keep safe from cost overruns :|
- Orchestration - Github actions
- [ ] Data warehouse setup? Dbt? Could use an alternative
- [x] Decide on API access method - requests direct pings 
- [ ] Write up scraping script and connect to data store
- [ ] Setup Github actions to orchestrate


