SELECT LEAD(c3) OVER( PARTITION BY c2 ORDER BY c1 ) LEAD_c3 FROM ( SELECT col1 c1, col2 c2, col3 c3 FROM "fewRowsAllData.parquet") sub_query
