SELECT LEAD(col7) OVER ( PARTITION BY col3 ORDER BY col7 ) LEAD_col7 FROM "fewRowsAllData.parquet"
