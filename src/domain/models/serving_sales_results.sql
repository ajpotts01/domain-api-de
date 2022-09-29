drop table if exists {{ target_table }}; 

create table {{ target_table }} as (
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
    FROM staging_sales_results
    ORDER BY city ASC, lastmodifieddate DESC;
);