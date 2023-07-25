SELECT LEAD(c2) OVER( PARTITION BY c2 ORDER BY c1 ) LEAD_c2 FROM ( SELECT col2 c2, col1 c1 FROM "fewRowsAllData.parquet") subquery
