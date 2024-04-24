
  
    
    
        
        insert into test.hash_appmetrica_registry__dbt_backup ("appmetricaDeviceId", "crmUserId", "cityName", "__emitted_at", "__table_name", "__link", "AppProfileMatchingHash", "AppMetricaDeviceHash", "CrmUserHash", "__id", "__datetime")
  -- depends_on: test.combine_appmetrica_registry
SELECT *,
  assumeNotNull(CASE 
WHEN __link = 'AppProfileMatching' 
    THEN AppProfileMatchingHash 

    END) as __id
  , assumeNotNull(CASE 

WHEN __link = 'UtmHashRegistry' 
        THEN toDateTime(toDateTime(0))
    







WHEN __link = 'AppProfileMatching' 
        THEN toDateTime(toDateTime(0))
    

    END) as __datetime

FROM (

SELECT 
    *, 
    
        
        assumeNotNull(coalesce(if(ifnull(nullif(upper(trim(toString(appmetricaDeviceId))), ''), '') || ifnull(nullif(upper(trim(toString(toDateTime(0)))), ''), '') = '', null, hex(MD5('AppProfileMatching' || ';' || ifnull(nullif(upper(trim(toString(appmetricaDeviceId))), ''), '') || ';' || ifnull(nullif(upper(trim(toString(toDateTime(0)))), ''), '')))))) as AppProfileMatchingHash
    ,
    
        
        assumeNotNull(coalesce(if(ifnull(nullif(upper(trim(toString(appmetricaDeviceId))), ''), '') = '', null, hex(MD5(ifnull(nullif(upper(trim(toString(appmetricaDeviceId))), ''), '')))))) as AppMetricaDeviceHash

,
    
        
        assumeNotNull(coalesce(if(ifnull(nullif(upper(trim(toString(crmUserId))), ''), '') = '', null, hex(MD5(ifnull(nullif(upper(trim(toString(crmUserId))), ''), '')))))) as CrmUserHash


    
FROM (

        (
            select
                            toString("appmetricaDeviceId") as appmetricaDeviceId ,
                            toString("crmUserId") as crmUserId ,
                            toString("cityName") as cityName ,
                            toDateTime("__emitted_at") as __emitted_at ,
                            toString("__table_name") as __table_name ,
                            toString("__link") as __link 

            from test.combine_appmetrica_registry
        )

        ) 
    WHERE 
    
        True
    )

-- SETTINGS short_circuit_function_evaluation=force_enable


  