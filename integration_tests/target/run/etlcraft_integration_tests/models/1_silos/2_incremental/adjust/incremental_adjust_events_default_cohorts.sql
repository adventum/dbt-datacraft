
        
  
    
    
        
        insert into test.incremental_adjust_events_default_cohorts__dbt_new_data_087659a2_aa28_455d_8cec_34f515445ad6 ("__date", "country", "date", "event_name", "event_token", "events", "network", "period", "tracker_token", "__table_name", "__emitted_at", "__normalized_at")
  -- depends_on: test.normalize_adjust_events_default_cohorts


SELECT * REPLACE(toDate(__date, 'UTC') AS __date) 

FROM test.normalize_adjust_events_default_cohorts
  
      