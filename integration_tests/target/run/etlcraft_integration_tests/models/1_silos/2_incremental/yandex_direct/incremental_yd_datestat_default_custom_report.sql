
        
  
    
    
        
        insert into test.incremental_yd_datestat_default_custom_report__dbt_new_data_0aa9aeaf_5cd9_435f_8908_a9af65d9d477 ("__date", "__clientName", "__productName", "AdId", "CampaignId", "CampaignName", "CampaignType", "Clicks", "Cost", "Date", "Impressions", "__table_name", "__emitted_at", "__normalized_at")
  -- depends_on: test.normalize_yd_datestat_default_custom_report


SELECT * REPLACE(toDate(__date, 'UTC') AS __date) 

FROM test.normalize_yd_datestat_default_custom_report
  
      