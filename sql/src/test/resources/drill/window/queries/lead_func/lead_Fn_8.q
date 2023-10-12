SELECT col7 , LEAD(col7 ) OVER ( PARTITION BY col2 ORDER BY col0 ) LEAD_col7 FROM "fewRowsAllData.parquet" WHERE col2 NOT IN ('NY')
