

  create view test.normalize_calltouch_events_default_calls__dbt_tmp 
  
  as (
    SELECT
        replaceRegexpOne(replaceRegexpOne(date, '\\s+(\\d):', ' 0\\1:'), '(\\d{2})\\/(\\d{2})\\/(\\d{4})', '\\3-\\2-\\1') AS __date, 
        JSONExtractString(_airbyte_data, 'additionalTags') AS additionalTags, 
        JSONExtractString(_airbyte_data, 'attribution') AS attribution, 
        JSONExtractString(_airbyte_data, 'attrs') AS attrs, 
        JSONExtractString(_airbyte_data, 'browser') AS browser, 
        JSONExtractString(_airbyte_data, 'callbackCall') AS callbackCall, 
        JSONExtractString(_airbyte_data, 'callbackInfo') AS callbackInfo, 
        JSONExtractString(_airbyte_data, 'callClientUniqueId') AS callClientUniqueId, 
        JSONExtractString(_airbyte_data, 'callerNumber') AS callerNumber, 
        JSONExtractString(_airbyte_data, 'callId') AS callId, 
        JSONExtractString(_airbyte_data, 'callphase') AS callphase, 
        JSONExtractString(_airbyte_data, 'callReferenceId') AS callReferenceId, 
        JSONExtractString(_airbyte_data, 'callUrl') AS callUrl, 
        JSONExtractString(_airbyte_data, 'city') AS city, 
        JSONExtractString(_airbyte_data, 'clientId') AS clientId, 
        JSONExtractString(_airbyte_data, 'ctCallerId') AS ctCallerId, 
        JSONExtractString(_airbyte_data, 'ctClientId') AS ctClientId, 
        JSONExtractString(_airbyte_data, 'ctGlobalId') AS ctGlobalId, 
        JSONExtractString(_airbyte_data, 'date') AS date, 
        JSONExtractString(_airbyte_data, 'dcm') AS dcm, 
        JSONExtractString(_airbyte_data, 'device') AS device, 
        JSONExtractString(_airbyte_data, 'duration') AS duration, 
        JSONExtractString(_airbyte_data, 'googleAdWords') AS googleAdWords, 
        JSONExtractString(_airbyte_data, 'hostname') AS hostname, 
        JSONExtractString(_airbyte_data, 'ip') AS ip, 
        JSONExtractString(_airbyte_data, 'keyword') AS keyword, 
        JSONExtractString(_airbyte_data, 'manager') AS manager, 
        JSONExtractString(_airbyte_data, 'mapVisits') AS mapVisits, 
        JSONExtractString(_airbyte_data, 'medium') AS medium, 
        JSONExtractString(_airbyte_data, 'order') AS order, 
        JSONExtractString(_airbyte_data, 'orders') AS orders, 
        JSONExtractString(_airbyte_data, 'os') AS os, 
        JSONExtractString(_airbyte_data, 'phoneNumber') AS phoneNumber, 
        JSONExtractString(_airbyte_data, 'phonesInText') AS phonesInText, 
        JSONExtractString(_airbyte_data, 'phrases') AS phrases, 
        JSONExtractString(_airbyte_data, 'redirectNumber') AS redirectNumber, 
        JSONExtractString(_airbyte_data, 'ref') AS ref, 
        JSONExtractString(_airbyte_data, 'sessionDate') AS sessionDate, 
        JSONExtractString(_airbyte_data, 'sessionId') AS sessionId, 
        JSONExtractString(_airbyte_data, 'sipCallId') AS sipCallId, 
        JSONExtractString(_airbyte_data, 'siteId') AS siteId, 
        JSONExtractString(_airbyte_data, 'siteName') AS siteName, 
        JSONExtractString(_airbyte_data, 'source') AS source, 
        JSONExtractString(_airbyte_data, 'statusDetails') AS statusDetails, 
        JSONExtractString(_airbyte_data, 'subPoolName') AS subPoolName, 
        JSONExtractString(_airbyte_data, 'successful') AS successful, 
        JSONExtractString(_airbyte_data, 'targetCall') AS targetCall, 
        JSONExtractString(_airbyte_data, 'timestamp') AS timestamp, 
        JSONExtractString(_airbyte_data, 'uniqTargetCall') AS uniqTargetCall, 
        JSONExtractString(_airbyte_data, 'uniqueCall') AS uniqueCall, 
        JSONExtractString(_airbyte_data, 'url') AS url, 
        JSONExtractString(_airbyte_data, 'userAgent') AS userAgent, 
        JSONExtractString(_airbyte_data, 'utmCampaign') AS utmCampaign, 
        JSONExtractString(_airbyte_data, 'utmContent') AS utmContent, 
        JSONExtractString(_airbyte_data, 'utmMedium') AS utmMedium, 
        JSONExtractString(_airbyte_data, 'utmSource') AS utmSource, 
        JSONExtractString(_airbyte_data, 'utmTerm') AS utmTerm, 
        JSONExtractString(_airbyte_data, 'waitingConnect') AS waitingConnect, 
        JSONExtractString(_airbyte_data, 'yaClientId') AS yaClientId, 
        JSONExtractString(_airbyte_data, 'yandexDirect') AS yandexDirect,
        toLowCardinality(_dbt_source_relation) AS __table_name,  
        toDateTime32(substring(toString(_airbyte_extracted_at), 1, 19)) AS __emitted_at, 
        NOW() AS __normalized_at
FROM (

(
SELECT
        toLowCardinality('datacraft_testValeriya_raw__stream_calltouch_default_accountid_calls') AS _dbt_source_relation,
        toString("_airbyte_raw_id") AS _airbyte_raw_id,
        toString("_airbyte_data") AS _airbyte_data,
        toString("_airbyte_extracted_at") AS _airbyte_extracted_at,
        toInt32("_airbyte_loaded_at") AS _airbyte_loaded_at
FROM test.datacraft_testValeriya_raw__stream_calltouch_default_accountid_calls
)

)
  )