SELECT col0 , LAG(col0 ) OVER ( PARTITION BY col2 ORDER BY col0 nulls FIRST ) LAG_col0 FROM "fewRowsAllData.parquet"
