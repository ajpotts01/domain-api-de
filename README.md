# Domain.com.au API ELT Pipeline for Sales per City - Data Engineering Camp 

## Overview

A dockerised data-engineering ELT pipeline for the [Domain Real Estate Data API](https://developer.domain.com.au). At time of writing, only sales results and listings have been supported.

## Requirements
The following items are required to build/run this project as intended:
1. A [Domain Developer](https://developer.domain.com.au) account and associated API key
2. A target database (only Postgres is supported at this time)
3. A method of running containers (e.g. Docker, or a relevant container engine on your cloud platform of choice)

## Get Started

1. Sign up for an account at the [Domain Developer Website](http://developer.domain.com.au)
2. Create an API key
3. Clone the repository, e.g. ```git clone https://github.com/Liutmantas/DEC-Project1.git```
4. Create an environment (.env) file containing the following items:
    - ```source_api_key```: Your Domain API key from Step 1
    - ```target_db_user```: Username for your target database
    - ```target_db_password```: Password for your target databsae
    - ```target_db_server_name```: URL of your target database, e.g. localhost if running locally, host.docker.internal if running in local container, or web address of your database in the cloud
    - ```target_database_name```: Database to load data to.
5. Supply a config.yaml file within /domain/ to target specific cities and/or APIs. A sample is provided.
6. Build the container from the root folder of the project, preferably tagging it with a name of choice, e.g.:
    - ```docker build . -t domain-dwh```
7. Run container, making sure to inject your environment file from Step 4, e.g.:
    - ```docker run --env-file ./domain_local.env domain-dwh:latest```

## Cloud Architecture Options
The following diagram shows a possible deployment on AWS:
<img src="domain_pipeline_aws_reference_architecture.png" alt="Reference deployment on AWS">

This architecture:
- Uses Amazon Elastic Container Service clusters and task definitions to orchestrate everything
- Amazon Elastic Container Registry to store the Docker image
- A .env file located in an S3 bucket that is only visible to the ECS cluster, for injecting environment variables.

## Configuration Options
There are four sections in the configuration file:
```yaml
- extract
- load
- transform
- meta
```

The following table describes each configuration entry.

| Section | Property      | Description
|:---     |:---           |:---
| extract | target_cities | List of cities to retrieve data for. Only cities supported by [the Domain endpoints](https://developer.domain.com.au/docs/latest/apis/pkg_properties_locations/references/salesresults_get).
| extract | api_urls      | API URLs. Only [aggregated sales results](https://developer.domain.com.au/docs/latest/apis/pkg_properties_locations/references/salesresults_get) and [listings](https://developer.domain.com.au/docs/latest/apis/pkg_properties_locations/references/salesresults_listings) have been accounted for at this time.
| load    | mode          | "full", "upsert", or "upsert_chunks". Incremental loads not supported. upsert_chunks recommended if using the listings API.
| load    | chunksize     | Chunksize (in bytes) to use for upserts.
| load    | key_columns   | A YAML dictionary specifying the key columns to use for loading the data after retrieval. The entries in key_columns and api_urls must be called the same thing - the pipeline expects them to match.
| transform | model_path | Path to SQL models for transformations.
| meta | log_table | Table to write logs to in the database.

### Configuration Notes

Although a sample config.yaml is provided, the following notes help explain some of the design choices:

- extract -> api_urls  
The keys/names for each API URL will be used as the raw table name in the database. For example:
```yaml
api_urls:
  sales_results: https://api.domain.com.au/v1/salesResults/{city}
  sales_listings: https://api.domain.com.au/v1/salesResults/{city}/listings
```
The pipeline will create two tables: raw_sales_results and raw_sales_listings. These tables will contain the combined data form all cities specified in extract -> target_cities.

- load -> key_columns  
The keys/names for this list MUST match the same list as above. The pipeline uses the names to pair up key columns with the raw tables so upserts can be performed.

The following YAML would be used to pair with the extract -> api_urls example:
```yaml
key_columns:
  sales_results:
    - city
    - lastModifiedDateTime
  sales_listings:
    - id
    - propertyDetailsUrl
    - result
```

## Acknowledgments

### Primary Contributors
- [ajpotts03](https://github.com/ajpotts03)
- [Liutmantas](https://github.com/Liutmantas)
- [tanhtra](https://github.com/tanhtra)
