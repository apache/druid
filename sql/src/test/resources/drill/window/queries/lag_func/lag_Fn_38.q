SELECT col1 , LAG(col1) OVER (ORDER BY col1 ) LAG_col1 FROM "fewRowsAllData.parquet" WHERE col1 <> 0
