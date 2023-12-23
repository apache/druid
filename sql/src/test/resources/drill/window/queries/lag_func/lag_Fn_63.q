SELECT c8 , LAG(c8) OVER( PARTITION BY c2 ORDER BY c8 ) LAG_c8 FROM ( SELECT col8 c8, col1 c1, col2 c2 FROM "fewRowsAllData.parquet" WHERE col8 < '07:10:06.550' ORDER BY col8) sub_query
