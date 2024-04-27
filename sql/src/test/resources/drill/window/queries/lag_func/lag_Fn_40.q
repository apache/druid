SELECT col3 , LAG(col3) OVER (ORDER BY col3 ) LAG_col3 FROM "fewRowsAllData.parquet"
