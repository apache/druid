SELECT col7 , LAG(col7) OVER (ORDER BY col7) LAG_col7 FROM "fewRowsAllData.parquet"
