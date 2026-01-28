SELECT
col2, col8,
LEAD(col8) OVER (PARTITION BY col2 ORDER BY col8 nulls FIRST) LEAD_col8
FROM "fewRowsAllData.parquet"
ORDER BY col2, col8
limit 10
