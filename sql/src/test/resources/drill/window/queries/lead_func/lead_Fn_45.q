SELECT col8 , LEAD(col8) OVER (ORDER BY col8) LEAD_col8 FROM "fewRowsAllData.parquet" WHERE col8 < '07:10:06.550'
