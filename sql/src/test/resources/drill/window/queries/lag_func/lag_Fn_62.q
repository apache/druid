SELECT c2 , c7 , LAG(c7) OVER( PARTITION BY c2 ORDER BY c7 ) LAG_c7 FROM ( SELECT col7 c7, col1 c1, col2 c2 FROM "fewRowsAllData.parquet" WHERE col2 NOT IN ('NY')) sub_query
