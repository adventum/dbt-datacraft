-- depends_on: test.join_appmetrica_registry
SELECT * REPLACE(toLowCardinality(__table_name) AS __table_name)
FROM (

        (
            select

                --toLowCardinality('join_appmetrica_registry')  as None,
                
                            toString("appmetricaDeviceId") as appmetricaDeviceId ,
                            toString("crmUserId") as crmUserId ,
                            toString("cityName") as cityName ,
                            toDateTime("__emitted_at") as __emitted_at ,
                            toString("__link") as __link 

            from test.join_appmetrica_registry
        )

        union all
        

        (
            select

                --toLowCardinality('join_appmetrica_registry_app_profile_matching')  as None,
                
                            toString("appmetricaDeviceId") as appmetricaDeviceId ,
                            toString("crmUserId") as crmUserId ,
                            toString("cityName") as cityName ,
                            toDateTime("__emitted_at") as __emitted_at ,
                            toString("__link") as __link 

            from test.join_appmetrica_registry_app_profile_matching
        )

        )