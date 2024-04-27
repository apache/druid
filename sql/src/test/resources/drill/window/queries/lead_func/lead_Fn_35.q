SELECT LEAD(col7,1) OVER ( PARTITION BY col2 ORDER BY col7 ) LEAD_col7 FROM "fewRowsAllData.parquet"
