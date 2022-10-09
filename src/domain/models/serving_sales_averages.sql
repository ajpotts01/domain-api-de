drop table if exists {{ target_table }}; 

create table {{ target_table }} as (
    SELECT
        date_part('year', lastmodifieddate) as lastmod_year, 
        city, 
        ROUND(AVG(adjclearancerate), 4) as clearancerate_average, 
        ROUND(AVG(median), 2) as median_average, 
        ROUND(AVG(numberauctioned), 2) as auctioned_average, 
        ROUND(AVG(numberlistedforauction), 2) as listed_average, 
        ROUND(AVG(numbersold), 2) as sold_average, 
        ROUND(AVG(numberunreported), 2) as unreported_average, 
        ROUND(AVG(numberwithdrawn), 2) as withdrawn_average, 
        ROUND(AVG(totalsales), 0) as sales_average
    FROM public.serving_sales_results
    GROUP BY city, lastmod_year
    ORDER BY lastmod_year DESC
);