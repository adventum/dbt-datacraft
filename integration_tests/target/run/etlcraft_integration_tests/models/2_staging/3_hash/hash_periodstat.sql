
        
  
    
    
        
        insert into test.hash_periodstat__dbt_new_data_e73065dd_cb2b_48cf_898d_f49bff11cfc3 ("__date", "campaign", "cost", "periodStart", "periodEnd", "__emitted_at", "__table_name", "__link", "ManualAdCostStatHash", "__id", "__datetime")
  -- depends_on: test.combine_periodstat
SELECT *,
  assumeNotNull(CASE  
    WHEN __link = 'ManualAdCostStat' 
    THEN ManualAdCostStatHash 

    END) as __id
  , assumeNotNull(CASE
    WHEN __link = 'ManualAdCostStat' 
    
    THEN toDateTime(__date) 
    END) AS __datetime
FROM (

SELECT *, 
assumeNotNull(coalesce(if(ifnull(nullif(upper(trim(toString(__date))), ''), '') || ifnull(nullif(upper(trim(toString(periodStart))), ''), '') || ifnull(nullif(upper(trim(toString(periodEnd))), ''), '') || ifnull(nullif(upper(trim(toString(__date))), ''), '') = '', null, hex(MD5('ManualAdCostStat' || ';' || ifnull(nullif(upper(trim(toString(__date))), ''), '') || ';' || ifnull(nullif(upper(trim(toString(periodStart))), ''), '') || ';' || ifnull(nullif(upper(trim(toString(periodEnd))), ''), '') || ';' || ifnull(nullif(upper(trim(toString(__date))), ''), '')))))) as ManualAdCostStatHash


FROM (

(
SELECT
        toDate("__date") as __date ,
        toString("campaign") as campaign ,
        toFloat64("cost") as cost ,
        toDate("periodStart") as periodStart ,
        toDate("periodEnd") as periodEnd ,
        toDateTime("__emitted_at") as __emitted_at ,
        toString("__table_name") as __table_name ,
        toString("__link") as __link 
FROM test.combine_periodstat
)

) 
WHERE 

    True
)

-- SETTINGS short_circuit_function_evaluation=force_enable


  
      