
        
  
    
    
        
        insert into test.incremental_adjust_events_default_cohorts__dbt_new_data_ad331357_6a84_4c95_8464_974832726b93 ("__date", "country", "date", "event_name", "event_token", "events", "network", "period", "tracker_token", "__table_name", "__emitted_at", "__normalized_at")
  -- depends_on: test.normalize_adjust_events_default_cohorts


SELECT * REPLACE(toDate(__date, 'UTC') AS __date) 

FROM test.normalize_adjust_events_default_cohorts
  
      