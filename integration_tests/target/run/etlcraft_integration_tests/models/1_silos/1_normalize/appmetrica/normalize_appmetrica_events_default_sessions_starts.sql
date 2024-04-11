

  create view test.normalize_appmetrica_events_default_sessions_starts__dbt_tmp 
  
  as (
    SELECT
        JSONExtractString(_airbyte_data, 'session_start_datetime') AS __date, 
JSONExtractString(_airbyte_data, '__clientName') AS __clientName, 
JSONExtractString(_airbyte_data, '__productName') AS __productName, 
JSONExtractString(_airbyte_data, 'appmetrica_device_id') AS appmetrica_device_id, 
JSONExtractString(_airbyte_data, 'city') AS city, 
JSONExtractString(_airbyte_data, 'google_aid') AS google_aid, 
JSONExtractString(_airbyte_data, 'installation_id') AS installation_id, 
JSONExtractString(_airbyte_data, 'ios_ifa') AS ios_ifa, 
JSONExtractString(_airbyte_data, 'os_name') AS os_name, 
JSONExtractString(_airbyte_data, 'profile_id') AS profile_id, 
JSONExtractString(_airbyte_data, 'session_start_datetime') AS session_start_datetime,
        toLowCardinality(_dbt_source_relation) AS __table_name,toDateTime32(substring(_airbyte_emitted_at, 1, 19)) AS __emitted_at, 
        NOW() as __normalized_at
    FROM (
    

        (
            select
                cast('test._airbyte_raw_appmetrica_events_default_testaccount_sessions_starts' as String) as _dbt_source_relation,

                
                    cast("_airbyte_ab_id" as String) as "_airbyte_ab_id" ,
                    cast("_airbyte_data" as String) as "_airbyte_data" ,
                    cast("_airbyte_emitted_at" as String) as "_airbyte_emitted_at" 

            from test._airbyte_raw_appmetrica_events_default_testaccount_sessions_starts

            
        )

        )
  )