SELECT col7 , LAG(col7 ) OVER ( PARTITION BY col2 ORDER BY col7 nulls FIRST ) LAG_col7 FROM "fewRowsAllData.parquet" WHERE col2 NOT IN ('NY')
