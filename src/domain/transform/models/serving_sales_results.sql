SELECT 
    city, 
    auctioneddate, 
    lastmodifieddate, 
    adjclearancerate, 
    median, 
    numberauctioned, 
    numberlistedforauction, 
    numbersold, 
    numberunreported, 
    numberwithdrawn, 
    totalsales
FROM {{ ref('staging_sales_results') }}
ORDER BY city ASC, lastmodifieddate DESC