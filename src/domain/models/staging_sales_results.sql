drop table if exists {{ target_table }}; 

create table {{ target_table }} as (
    SELECT 
        CAST("City" AS TEXT) as City, 
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
    FROM raw_sales_result
); 

