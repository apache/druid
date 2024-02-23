SELECT LEAD(c6) OVER( PARTITION BY c2 ORDER BY c1 ) LEAD_c6 FROM ( SELECT col6 c6, col1 c1, col2 c2 FROM "fewRowsAllData.parquet") sub_query
