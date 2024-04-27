SELECT c7, LEAD(c7) OVER( PARTITION BY c2 ORDER BY c7 ) LEAD_c7, c2 FROM ( SELECT col7 c7, col1 c1, col2 c2 FROM "fewRowsAllData.parquet") sub_query
