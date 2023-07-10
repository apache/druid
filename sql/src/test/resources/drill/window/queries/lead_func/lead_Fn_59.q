SELECT LEAD(c4) OVER( PARTITION BY c2 ORDER BY c1 ) LEAD_c4 FROM ( SELECT col4 c4, col1 c1, col2 c2 FROM "fewRowsAllData.parquet") sub_query
