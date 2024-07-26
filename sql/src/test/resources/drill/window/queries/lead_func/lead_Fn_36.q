SELECT col8 , LEAD(col8,1) OVER ( PARTITION BY col2 ORDER BY col8 ) LEAD_col8, col2
FROM "fewRowsAllData.parquet"
where
    col8 < TIMESTAMP_TO_MILLIS(TIME_PARSE('08:03:53.340', 'HH:mm:ss.SSS')) and
    col2 NOT IN ('WI','WY','OR','PA','RI','NE','IN','NC','MO','HI','GA','CO')
