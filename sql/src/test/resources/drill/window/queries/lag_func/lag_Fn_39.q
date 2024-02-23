SELECT col2 , LAG(col2) OVER (ORDER BY col2 ) LAG_col2 FROM "fewRowsAllData.parquet"
