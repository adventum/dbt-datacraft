-- depends_on: test.join_sheets_periodstat
SELECT * REPLACE(toLowCardinality(__table_name) AS __table_name)
FROM (

        (
            select
                            toDate("__date") as __date ,
                            toString("campaign") as campaign ,
                            toFloat64("cost") as cost ,
                            toDate("periodStart") as periodStart ,
                            toDate("periodEnd") as periodEnd ,
                            toDateTime("__emitted_at") as __emitted_at ,
                            toString("__table_name") as __table_name ,
                            toString("__link") as __link 

            from test.join_sheets_periodstat
        )

        ) 

