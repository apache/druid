SELECT col4 , LAG(col4) OVER (ORDER BY col4) LAG_col4
FROM "fewRowsAllData.parquet"
WHERE col4 < TIMESTAMP_TO_MILLIS(TIME_PARSE('2012-06-02 00:28:02.433', 'yyyy-MM-dd HH:mm:ss.SSS'))
