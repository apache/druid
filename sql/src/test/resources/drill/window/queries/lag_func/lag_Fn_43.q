SELECT col6 , LAG(col6) OVER (ORDER BY col6 ) LAG_col6 FROM "fewRowsAllData.parquet"
