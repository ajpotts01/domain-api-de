# Domain.com.au API ELT Pipeline - Data Engineering Camp 

## Overview

A dockerised data-engineering ELT pipeline for the [Domain Real Estate Data API](https://developer.domain.com.au).

## Requirements
The following items are required to build/run this project as intended:
1. A [Domain Developer](https://developer.domain.com.au) account and associated API key
2. A target database (only Postgres is supported at this time)
3. A method of running containers (e.g. Docker, or a relevant container engine on your cloud platform of choice)

## Get Started

1. Sign up for an account at the [Domain Developer Website](http://developer.domain.com.au)
2. Create an API key
3. Clone the repository
4. Create an environment (.env) file containing the following items:
    - source_api_key: Your Domain API key from Step 1
    - target_db_user: Username for your target database
    - target_db_password: Password for your target databsae
    - target_db_server_name: URL of your target database, e.g. localhost if running locally, host.docker.internal if running in local container, or web address of your database in the cloud
    - target_database_name: Database to load data to.
5. Supply a config.yaml file within /domain/ to target specific cities and/or APIs. A sample is provided.
6. Build the container from the root folder of the project, preferably tagging it with a name of choice, e.g.:
    - docker build . -t domain-dwh
7. Run container, making sure to inject your environment file from Step 4, e.g.:
    - docker run --env-file ./domain_local.env domain-dwh:latest

## Configuration Options


### Modify targeted cities - Config.yaml

Adding or removing cities from the target_cities section will add the city into the pipeline

```yaml
target_cities:
- Sydney
- Melbourne
- Adelaide
```

### Modify targeted APIs - Config.yaml

TBD

```yaml
api_urls:
    salesResults: https://api.domain.com.au/v1/salesResults/{city}
    salesResults_listings: https://api.domain.com.au/v1/salesResults/{city}/listings
```

## Acknowledgments

### Primary Contributors
- [ajpotts03](https://github.com/ajpotts03)
- [Liutmantas](https://github.com/Liutmantas)
- [tanhtra](https://github.com/tanhtra)
