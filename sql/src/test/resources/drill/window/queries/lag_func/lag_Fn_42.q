SELECT col5 , LAG(col5) OVER (ORDER BY col5) LAG_col5
FROM "fewRowsAllData.parquet"
WHERE col5 < TIMESTAMP_TO_MILLIS(TIME_PARSE('1968-06-06', 'yyyy-MM-dd'))
