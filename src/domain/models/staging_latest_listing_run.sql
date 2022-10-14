drop table if exists {{ target_table }}; 

create table {{ target_table }} as (

    SELECT 
        id, 
        result, 
        price, 
        propertydetailsurl, 
        propertytype, 
        state, 
        postcode, 
        unitnumber, 
        streetnumber, 
        streetname, 
        streettype, 
        suburb, 
        bathrooms, 
        bedrooms, 
        carspaces, 
        lat, 
        long, 
        agencyid, 
        agencyname, 
        agencydetailsurl, 
        agent, 
        url, 
        execution_time
    FROM public.staging_sales_listings
    WHERE execution_time >= (
        SELECT TO_TIMESTAMP(run_timestamp, 'YYYY-MM-DDTHH24:MI:ss') as run_timestamp
        FROM public.pipeline_logs
        WHERE run_status = 'started' 
        AND run_id = (
                    SELECT max(run_id) - 1 FROM pipeline_logs
                    WHERE run_status = 'completed')
    )
)