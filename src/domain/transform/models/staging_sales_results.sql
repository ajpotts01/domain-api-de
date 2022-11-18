SELECT 
    CAST("city" AS TEXT) as city, 
    TO_TIMESTAMP("auctionedDate", 'YYYY-MM-DD')::date as auctioneddate,
    TO_TIMESTAMP("lastModifiedDateTime", 'YYYY-MM-DDTHH24:MI:ssZ') as lastmodified,
    TO_TIMESTAMP("lastModifiedDateTime", 'YYYY-MM-DDTHH24:MI:ssZ')::date as lastmodifieddate,
    ROUND(cast("adjClearanceRate" as numeric),3) as adjclearancerate, 
    median, 
    "numberAuctioned" as numberauctioned, 
    "numberListedForAuction" as numberlistedforauction, 
    "numberSold" as numbersold, 
    "numberUnreported" as numberunreported, 
    "numberWithdrawn" as numberwithdrawn, 
    "totalSales" as totalsales
FROM {{ source('domain_dwh', 'raw_sales_results') }}