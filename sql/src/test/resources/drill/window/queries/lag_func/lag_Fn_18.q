SELECT
col2, col8,
LAG(col8) OVER (PARTITION BY col2 ORDER BY col2, col8 nulls FIRST) LAG_col8
FROM "fewRowsAllData.parquet"
ORDER BY col2, col8
FETCH FIRST 15 ROWS ONLY
