drop table if exists {{ target_table }}; 

create table {{ target_table }} as (
    SELECT 
        id,
        result,
        price,
        "propertyDetailsUrl" as propertydetailsurl,
        "propertyType" as propertytype,
        state,
        postcode,
        "unitNumber" as unitnumber,
        "streetNumber" as streetnumber,
        "streetName" as streetname,
        "streetType" as streettype,
        suburb,
        bathrooms,
        bedrooms,
        carspaces,
        "geoLocation.latitude" as lat,
        "geoLocation.longitude" as long,
        "agencyId" as agencyid,
        "agencyName" as agencyname,
        "agencyProfilePageUrl" as agencydetailsurl,
        agent,
        url, 
        TO_TIMESTAMP("execution_time", 'YYYY-MM-DDTHH24:MI:ssZ') as execution_time
    FROM raw_sales_listings
);