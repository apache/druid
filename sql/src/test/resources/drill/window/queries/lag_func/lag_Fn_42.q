SELECT col5 , LAG(col5) OVER (ORDER BY col5) LAG_col5 FROM "fewRowsAllData.parquet" WHERE col5 < '1968-06-06'
