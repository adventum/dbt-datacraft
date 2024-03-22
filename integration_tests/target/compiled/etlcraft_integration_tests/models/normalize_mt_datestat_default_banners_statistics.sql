SELECT
        JSONExtractString(_airbyte_data, 'date') AS __datetime, 
JSONExtractString(_airbyte_data, '__clientName') AS __clientName, 
JSONExtractString(_airbyte_data, '__productName') AS __productName, 
JSONExtractString(_airbyte_data, 'banner_id') AS banner_id, 
JSONExtractString(_airbyte_data, 'base') AS base,
        toLowCardinality(_dbt_source_relation) AS __table_name,toDateTimeOrDefault(_airbyte_emitted_at) AS __emitted_at, 
        NOW() as __normalized_at
    FROM (
    

        (
            select
                cast('test._airbyte_raw_mt_datestat_default_maxiretail_banners_statistics' as String) as _dbt_source_relation,

                
                    cast("_airbyte_ab_id" as String) as "_airbyte_ab_id" ,
                    cast("_airbyte_data" as String) as "_airbyte_data" ,
                    cast("_airbyte_emitted_at" as Date) as "_airbyte_emitted_at" 

            from test._airbyte_raw_mt_datestat_default_maxiretail_banners_statistics

            
        )

        )