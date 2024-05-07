

  create view test.normalize_mt_datestat_default_banners__dbt_tmp 
  
  as (
    SELECT
        JSONExtractString(_airbyte_data, '__clientName') AS __clientName, 
        JSONExtractString(_airbyte_data, '__productName') AS __productName, 
        JSONExtractString(_airbyte_data, 'campaign_id') AS campaign_id, 
        JSONExtractString(_airbyte_data, 'id') AS id, 
        JSONExtractString(_airbyte_data, 'textblocks') AS textblocks, 
        JSONExtractString(_airbyte_data, 'urls') AS urls,
        toLowCardinality(_dbt_source_relation) AS __table_name,  
        toDateTime32(substring(_airbyte_emitted_at, 1, 19)) AS __emitted_at, 
        NOW() AS __normalized_at
FROM (

(
SELECT
        toLowCardinality('_airbyte_raw_mt_datestat_default_testaccount_banners') AS _dbt_source_relation,
        toString("_airbyte_ab_id") AS _airbyte_ab_id ,
        toString("_airbyte_data") AS _airbyte_data ,
        toString("_airbyte_emitted_at") AS _airbyte_emitted_at 
FROM test._airbyte_raw_mt_datestat_default_testaccount_banners
)

)
  )