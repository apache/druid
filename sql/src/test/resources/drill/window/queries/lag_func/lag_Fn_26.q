SELECT col7 , LAG(col7 ) OVER ( PARTITION BY col2 ORDER BY col2,col7 nulls LAST ) LAG_col7, col2 FROM "fewRowsAllData.parquet"
