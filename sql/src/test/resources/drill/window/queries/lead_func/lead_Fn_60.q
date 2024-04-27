SELECT LEAD(c5) OVER( PARTITION BY c2 ORDER BY c1 ) LEAD_c5 FROM ( SELECT col5 c5, col1 c1, col2 c2 FROM "fewRowsAllData.parquet") sub_query
