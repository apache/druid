SELECT col1, col2, LAG ( col1 , 1) OVER ( PARTITION BY col2 ORDER BY col1 asc nulls FIRST ) w_LAG FROM "fewRowsAllData.parquet" WHERE col2 NOT IN ('CA','WI','AZ')
