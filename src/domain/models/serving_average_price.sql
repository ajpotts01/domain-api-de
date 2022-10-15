drop table if exists {{ target_table }}; 

create table {{ target_table }} as (
    SELECT 
        state, 
        suburb, 
        propertytype, 
        ROUND(AVG(price), 0) average_price,
        COUNT(*) count
    FROM public.staging_sales_listings
        WHERE result NOT IN ('AUWD', 'AUVB', 'PTSW', 'PTLA', 'AUPI', 'AUHB')
        AND price IS NOT NULL
        AND state IS NOT NULL
        GROUP BY state, suburb, propertytype
        ORDER BY state, suburb, propertytype
);