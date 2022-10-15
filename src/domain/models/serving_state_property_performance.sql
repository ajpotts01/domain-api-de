drop table if exists {{ target_table }}; 

create table {{ target_table }} as (
    SELECT 
        state, 
        propertytype, 
        bedrooms, 
        ROUND(AVG(price), 0) 
        average_price, 
        ROUND(STDDEV(price), 0) 
        price_std, 
        COUNT(*) count
    FROM public.staging_sales_listings
        WHERE result NOT IN ('AUWD', 'AUVB', 'PTSW', 'PTLA', 'AUPI', 'AUHB')
        AND price IS NOT NULL
        AND state IS NOT NULL
        GROUP BY state, propertytype, bedrooms
        ORDER BY state, propertytype, bedrooms DESC
);