SELECT col1, col2, LEAD ( col1 , 1) OVER ( PARTITION BY col2 ORDER BY col1 asc nulls FIRST ) w_LEAD FROM "fewRowsAllData.parquet" WHERE col2 NOT IN ('CA','WI','AZ')
