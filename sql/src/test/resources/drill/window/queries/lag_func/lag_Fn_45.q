SELECT col8 , LAG(col8) OVER (ORDER BY col8 ) LAG_col8 FROM "fewRowsAllData.parquet" WHERE col8 < '07:10:06.550'
