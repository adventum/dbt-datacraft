SELECT
        JSONExtractString(_airbyte_data, 'Period_start') AS __date, 
        JSONExtractString(_airbyte_data, 'Campaign') AS Campaign, 
        JSONExtractString(_airbyte_data, 'Cost') AS Cost, 
        JSONExtractString(_airbyte_data, 'Period_end') AS Period_end, 
        JSONExtractString(_airbyte_data, 'Period_start') AS Period_start,
        toLowCardinality(_dbt_source_relation) AS __table_name,  
        toDateTime32(substring(_airbyte_emitted_at, 1, 19)) AS __emitted_at, 
        NOW() AS __normalized_at
FROM (

(
SELECT
        toLowCardinality('_airbyte_raw_sheets_periodstat_default_testaccount_planCosts') AS _dbt_source_relation,
        toString("_airbyte_ab_id") AS _airbyte_ab_id ,
        toString("_airbyte_data") AS _airbyte_data ,
        toString("_airbyte_emitted_at") AS _airbyte_emitted_at 
FROM test._airbyte_raw_sheets_periodstat_default_testaccount_planCosts
)

)