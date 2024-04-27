SELECT col7 , col6, LAG(col6) OVER(PARTITION BY col7 ORDER BY col6) LAG_col6
FROM "allTypsUniq.parquet"
WHERE
    col6 > TIMESTAMP_TO_MILLIS(TIME_PARSE('1947-05-12', 'yyyy-MM-dd')) and
    col6 < TIMESTAMP_TO_MILLIS(TIME_PARSE('2007-10-01', 'yyyy-MM-dd'))