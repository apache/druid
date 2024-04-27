SELECT MAX(cast( col4 as TIMESTAMP )) OVER(PARTITION BY cast( col2 as CHAR(2)) ORDER BY cast( col0 as INT )) FROM "fewRowsAllData.parquet"
