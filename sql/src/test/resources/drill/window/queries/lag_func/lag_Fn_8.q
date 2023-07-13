SELECT col7 , LAG(col7 ) OVER ( PARTITION BY col2 ORDER BY col7 ) LAG_col7 ,col2 FROM "fewRowsAllData.parquet"
