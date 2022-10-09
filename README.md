# Domain.com.au API ELT Pipeline - Data Engineering Camp 

## Overview

A dockerised data-engineering ELT pipeline for the [Domain Real Estate Data API](https://developer.domain.com.au).

## Get Started

1. Get the container from these location(s)
    - Location 1
    - Location 2
    - Location 3
2. Modify the secrets_config.template.py
    - Rename the file to secrets_config.py
    - Configure secrets_config.py with the api_key from the [Domain Developer Portal](https://developer.domain.com.au) 
    - Configure secrets_config.py with preferred postgres database credentials.
3. Modify the Config.Yaml file within /domain/ to target specific cities and/or APIs
4. Run container

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
