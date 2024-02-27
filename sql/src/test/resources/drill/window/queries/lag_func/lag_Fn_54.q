SELECT col8 , LAG(col8 , 1) OVER ( PARTITION BY col3 ORDER BY col3,col8 nulls LAST ) LAG_col8 FROM "fewRowsAllData.parquet" fetch first 10 rows only
